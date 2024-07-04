import { appendFile, read, stat } from "@dr.pogodin/react-native-fs";
import thrift, { Int64, TTransportCallback } from "thrift";
import * as parquet_thrift from '../gen-nodejs/parquet_types';
import { FileMetaDataExt, WriterOptions } from './declare';

// Use this so users only need to implement the minimal amount of the WriteStream interface
export type WriteStreamMinimal = {
  write: (buf: Buffer) => Promise<void>;
  end: () => Promise<void>;
};

/**
 * We need to patch Thrift's TFramedTransport class bc the TS type definitions
 * do not define a `readPos` field, even though the class implementation has
 * one.
 */
class fixedTFramedTransport extends thrift.TFramedTransport {
  inBuf: Buffer
  readPos: number
  constructor(inBuf: Buffer) {
    super(inBuf)
    this.inBuf = inBuf
    this.readPos = 0
  }
}

type Enums = typeof parquet_thrift.Encoding | typeof parquet_thrift.FieldRepetitionType | typeof parquet_thrift.Type | typeof parquet_thrift.CompressionCodec | typeof parquet_thrift.PageType | typeof parquet_thrift.ConvertedType;

type ThriftObject = FileMetaDataExt | parquet_thrift.PageHeader | parquet_thrift.ColumnMetaData | parquet_thrift.BloomFilterHeader | parquet_thrift.OffsetIndex | parquet_thrift.ColumnIndex | FileMetaDataExt;

/** Patch PageLocation to be three element array that has getters/setters
  * for each of the properties (offset, compressed_page_size, first_row_index)
  * This saves space considerably as we do not need to store the full variable
  * names for every PageLocation
  */

const getterSetter = (index: number) => ({
  get: function(this: Array<number>): number { return this[index]; },
  set: function(this: Array<number>, value: number): number { return this[index] = value;}
});

Object.defineProperty(parquet_thrift.PageLocation.prototype,'offset', getterSetter(0));
Object.defineProperty(parquet_thrift.PageLocation.prototype,'compressed_page_size', getterSetter(1));
Object.defineProperty(parquet_thrift.PageLocation.prototype,'first_row_index', getterSetter(2));

/**
 * Helper function that serializes a thrift object into a buffer
 */
export const serializeThrift = function(obj: ThriftObject) {
  let output:Array<Uint8Array> = []

  const callBack:TTransportCallback = function (buf: Buffer | undefined) {
    output.push(buf as Buffer)
  }

  let transport = new thrift.TBufferedTransport(undefined, callBack)

  let protocol = new thrift.TCompactProtocol(transport)
  //@ts-ignore, https://issues.apache.org/jira/browse/THRIFT-3872
  obj.write(protocol)
  transport.flush()

  return Buffer.concat(output)
}

export const decodeThrift = function(obj: ThriftObject, buf: Buffer, offset?: number) {
  if (!offset) {
    offset = 0
  }

  var transport = new fixedTFramedTransport(buf);
  transport.readPos = offset;
  var protocol = new thrift.TCompactProtocol(transport);
  //@ts-ignore, https://issues.apache.org/jira/browse/THRIFT-3872
  obj.read(protocol);
  return transport.readPos - offset;
}

/**
 * Get the number of bits required to store a given value
 */
export const getBitWidth = function(val: number) {
  if (val === 0) {
    return 0;
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
}

/**
 * FIXME not ideal that this is linear
 */
export const getThriftEnum = function(klass: Enums, value: unknown) {
  for (let k in klass) {
    if (klass[k] === value) {
      return k;
    }
  }

  throw 'Invalid ENUM value';
}

export const fopen = async (filePath: string): Promise<string> => {
  return filePath;
}

export const fstat = async (filePath: string): Promise<{size:number}> => {
  return await stat(filePath);
}

export const fread = async (filePath: string, position: number, length: number): Promise<Buffer> => {
  const str = await read(filePath, length, position, {encoding: 'base64'});
  return Buffer.from(str, 'base64');
}

export const fclose = async (filePath: string) => {
  // noop
}

export const oswrite = async (os: WriteStreamMinimal, buf: Buffer) => {
  await os.write(buf);
}

export const osend = async (os: WriteStreamMinimal) => {
  await os.end();
}

export const osopen = async (path: string, opts?: WriterOptions): Promise<WriteStreamMinimal> => {
  return {
    write: async (buf: Buffer) => {
      await appendFile(path, buf.toString('base64'), {encoding: 'base64'});
    },
    end: async () => {
      // noop
    }
  };
}

export const fieldIndexOf = function(arr: Array<Array<unknown>>, elem: Array<unknown>) {
  for (let j = 0; j < arr.length; ++j) {
    if (arr[j].length !== elem.length) {
      continue;
    }

    let m = true;
    for (let i = 0; i < elem.length; ++i) {
      if (arr[j][i] !== elem[i]) {
        m = false;
        break;
      }
    }

    if (m) {
      return j;
    }
  }

  return -1;
}

export const cloneInteger = (int: Int64) => {
   return new Int64(int.valueOf());
};
