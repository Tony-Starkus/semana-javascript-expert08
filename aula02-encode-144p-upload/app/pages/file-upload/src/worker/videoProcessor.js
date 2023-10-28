
export default class VideoProcessor {
  #mp4Demuxer
  #webMWriter
  #service
  #buffers = []

  /**
   * 
   * @param {object} options 
   * @param {import('./mp4Demuxer.js').default} options.mp4Demuxer
   * @param {import('../deps/webm-writer2.js').default} options.webMWriter
   * @param {import('./service.js').default} options.service
   */

  constructor({ mp4Demuxer, webMWriter, service }) {
    this.#mp4Demuxer = mp4Demuxer
    this.#webMWriter = webMWriter
    this.#service = service
  }

  /** @returns {ReadableStream} */
  mp4Decoder(stream) {

    return new ReadableStream({
      start: async (controller) => {



        const decoder = new VideoDecoder({
          /** @param {VideoFrame} frame */
          output(frame) {
            controller.enqueue(frame)
          },
          error(e) {
            console.error('error at mp4Decoder', e)
            controller.error(e)
          }
        })

        return this.#mp4Demuxer.run(stream, {
          async onConfig(config) {
            // const { supported } = await VideoDecoder.isConfigSupported(config)
            // if (!supported) {
            //   console.error('mp4Muxer VideoDecoder config not suported!', config)
            //   controller.error()
            //   return
            // }
            decoder.configure(config)
          },
          /**
           * 
           * @param {EncodedVideoChunk} chunk 
           */
          onChunk(chunk) {
            decoder.decode(chunk)
          }
        })
        // .then(() => {
        //   setTimeout(() => {
        //     controller.close()
        //   }, 3000)
        // })
      }
    })
  }

  encode144p(encoderConfig) {
    let _encode;

    const readable = new ReadableStream({
      start: async (controller) => {
        const { supported } = await VideoEncoder.isConfigSupported(encoderConfig)
        if (!supported) {
          const message = 'encode144p VideoEncoder config not suported!'
          console.error(message, encoderConfig)
          controller.error(message)
          return
        }

        _encode = new VideoEncoder({
          /**
           * 
           * @param {EncodedVideoChunk} frame 
           * @param {EncodedVideoChunkMetadata} config 
           */
          output: (frame, config) => {
            if (config.decoderConfig) {
              const decoderConfig = {
                type: 'config',
                config: config.decoderConfig
              }

              controller.enqueue(decoderConfig)
            }
            controller.enqueue(frame)
          },
          error: (err) => {
            console.error('VideoEnconder 144p', err);
            controller.error()
          }
        })


        await _encode.configure(encoderConfig)


      }
    })

    const writable = new WritableStream({
      async write(frame) {
        _encode.encode(frame)
        frame.close()
      }
    })

    return {
      readable,
      writable
    }
  }

  renderDecodedFramesAndGetEncodedChunks(renderFrame) {
    let _decoder;
    return new TransformStream({
      start: (controller) => {
        _decoder = new VideoDecoder({
          output(frame) {
            renderFrame(frame)
          },
          error(e) {
            console.error('error at renderFrames', e)
            controller.error(e)
          }
        });
      },
      /**
       * 
       * @param {EncodedVideoChunk} encodedChunk 
       * @param {TransformStreamDefaultController} controller 
       */
      async transform(encodedChunk, controller) {
        if (encodedChunk.type === 'config') {
          await _decoder.configure(encodedChunk.config)
          return;
        }
        _decoder.decode(encodedChunk)

        // Need the encoded version to use webM
        controller.enqueue(encodedChunk)
      }
    })
  }

  transformIntoWebM() {
    const writable = new WritableStream({
      write: (chunk) => {
        this.#webMWriter.addFrame(chunk)
      },
      close() {
      },
    })

    return {
      readable: this.#webMWriter.getStream(),
      writable
    }
  }

  upload(filename, resolution, type) {
    const chunks = []
    let byteCount = 0
    let segmentCount = 0;

    const triggerUpload = async chunks => {
      const blob = new Blob(chunks, { type: 'video/webm' })

      // fazer upload
      this.#service.uploadFile({ filename: `${filename}-${resolution}.${++segmentCount}.${type}`, fileBuffer: blob })

      // Gambiara para remover os elementos do array, mas funciona.
      chunks.length = 0
      byteCount = 0
    }

    return new WritableStream({
      /**
       * 
       * @param {object} options
       * @param {Uint8Array} options.data 
       */
      async write({ data }) {
        chunks.push(data)
        byteCount += data.byteLength

        // Se for menor que 10mb, não faz o upload
        if (byteCount <= 10e6) return
        await triggerUpload(chunks)
      },
      async close() {
        if (!chunks.length) return
        await triggerUpload(chunks)
      }
    })
  }

  async start({ file, encoderConfig, renderFrame, sendMessage }) {
    const stream = file.stream();
    const fileName = file.name.split("/").pop().replace(".mp4", "");
    await this.mp4Decoder(stream)
      .pipeThrough(this.encode144p(encoderConfig))
      .pipeThrough(this.renderDecodedFramesAndGetEncodedChunks(renderFrame))
      .pipeThrough(this.transformIntoWebM())
      // .pipeThrough(new TransformStream({
      //   transform: ({ data, position }, controller) => {
      //     this.#buffers.push(data)
      //     controller.enqueue(data)
      //   },
      //   flush: () => {
      //     // sendMessage({
      //     //   status: 'done',
      //     //   buffers: this.#buffers,
      //     //   fileName: fileName.concat('-144p.webm')
      //     // })
      //     sendMessage({
      //       status: 'done'
      //     })
      //   }
      // }))
      .pipeTo(this.upload(fileName, '144p', 'webm'))
    sendMessage({
      status: 'done'
    })

  }
}