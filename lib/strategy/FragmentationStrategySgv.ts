import type { Quad } from '@rdfjs/types';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import rdfParser from 'rdf-parse';
import type { IQuadSink } from '../io/IQuadSink';
import { FragmentationStrategyStreamAdapter } from './FragmentationStrategyStreamAdapter';

const fs = require('node:fs');

const DF = new DataFactory<RDF.Quad>();

export class FragmentationStrategySgv extends FragmentationStrategyStreamAdapter {
  private readonly fragmentationPredicate = DF.namedNode('http://localhost:3000/internal/postsFragmentation');
  private readonly turtleSource = './sgv-descr.ttl';

  private readonly turtleStore: Quad[] = [];

  private readonly readPromise = new Promise<Quad[]>((resolve, reject) => {
    rdfParser.parse(fs.createReadStream(this.turtleSource), {
      contentType: 'text/turtle',
    }).on('data', (quad: Quad) => {
      this.turtleStore.push(quad);
    }).on('error', reject).on('end', () => resolve(this.turtleStore));
  });

  public constructor() {
    super();
  }

  public async handleQuad(quad: Quad, quadSink: IQuadSink): Promise<void> {
    if (quad.predicate.equals(this.fragmentationPredicate)) {
      const quads = await this.readPromise;

      const subjectIri = quad.subject.value.replace(/profile\/card#me$/u, 'sgv');

      for (const turtleQuad of quads) {
        await quadSink.push(
          subjectIri,
          DF.quad(
            DF.namedNode(subjectIri + turtleQuad.subject.value),
            turtleQuad.predicate,
            turtleQuad.object,
            turtleQuad.graph,
          ),
        );
      }
    }
  }
}
