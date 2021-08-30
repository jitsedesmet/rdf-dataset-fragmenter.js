import type { Readable } from 'stream';
import { PassThrough } from 'stream';
import type * as RDF from '@rdfjs/types';
import type { IQuadSource } from './IQuadSource';

/**
 * A quad source that combines multiple quad sources.
 *
 * Concretely, this will create a concatenated stream of all the given sources streams.
 */
export class QuadSourceComposite implements IQuadSource {
  private readonly sources: IQuadSource[];

  public constructor(sources: IQuadSource[]) {
    this.sources = sources;
  }

  public getQuads(): RDF.Stream & Readable {
    const concat = new PassThrough({ objectMode: true });
    let endedStreams = 0;
    for (const source of this.sources) {
      const stream = source.getQuads();
      stream.pipe(concat, { end: false });
      stream.on('error', error => concat.emit('error', error));
      // eslint-disable-next-line @typescript-eslint/no-loop-func
      stream.on('end', () => {
        if (++endedStreams === this.sources.length) {
          concat.push(null);
        }
      });
    }

    // Special case when we have no sources
    if (this.sources.length === 0) {
      concat.push(null);
    }

    return concat;
  }
}
