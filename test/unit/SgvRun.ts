import * as Path from 'path';
import { runConfig } from '../../lib/CliRunner';

describe('sgv', () => {
  it('run', async() => {
    await runConfig('fragmenter-config-pod.json', {
      mainModulePath: Path.join(__dirname, '../..'),
    });
  }, 2 ** 31 - 1);
});
