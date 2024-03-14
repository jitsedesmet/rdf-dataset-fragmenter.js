import type { Quad } from '@rdfjs/types';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import rdfParser from 'rdf-parse';
import type { IQuadSink } from '../io/IQuadSink';
import { FragmentationStrategyStreamAdapter } from './FragmentationStrategyStreamAdapter';

const fs = require('node:fs');

const DF = new DataFactory<RDF.Quad>();

export class FragmentationStrategySgv extends FragmentationStrategyStreamAdapter {
  private readonly fragmentationPredicate;
  private readonly fragmentationObject;

  private readonly readPromise: Promise<Quad[]>;

  public constructor(
    private readonly sgvTemplatePath: string,
    private readonly predicate: string,
    private readonly object: string,
    private readonly subjectRegexMatch: string,
  ) {
    super();
    const turtleStore: Quad[] = [];
    this.fragmentationPredicate = DF.namedNode(predicate);
    this.fragmentationObject = DF.namedNode(object);
    this.readPromise = new Promise<Quad[]>((resolve, reject) => {
      rdfParser.parse(fs.createReadStream(sgvTemplatePath), {
        contentType: 'text/turtle',
      }).on('data', (quad: Quad) => {
        turtleStore.push(quad);
      }).on('error', reject).on('end', () => resolve(turtleStore));
    });
  }

  public async handleQuad(quad: Quad, quadSink: IQuadSink): Promise<void> {
    if (quad.predicate.equals(this.fragmentationPredicate) && quad.object.equals(this.fragmentationObject)) {
      const quads = await this.readPromise;

      const subjectIri = quad.subject.value.replace(new RegExp(this.subjectRegexMatch, 'u'), 'sgv');

      for (const turtleQuad of quads) {
        await quadSink.push(
          subjectIri,
          DF.quad(
            turtleQuad.subject.termType === 'BlankNode' ?
              turtleQuad.subject :
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
