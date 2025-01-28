import { Transform } from "stream";

// Функция для создания Transform потока, разбивающего данные на чанки
export function chunkStream(chunkSize: number): Transform {
  return new Transform({
    transform(chunk: Buffer, _enc, callback) {
      let offset = 0;
      while (offset < chunk.length) {
        const end = offset + chunkSize;
        this.push(chunk.subarray(offset, end));
        offset = end;
      }
      callback();
    },

    flush(callback) {
      callback();
    },
  });
}

// Обработка чанка (заглушка)
export function processChunk(chunk: Buffer): Buffer {
  // Здесь может быть ваша логика обработки данных
  return chunk;
}
