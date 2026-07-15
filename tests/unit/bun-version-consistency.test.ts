import { readdir, readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';

const root = new URL('../../', import.meta.url);

function requiredMatch(contents: string, pattern: RegExp, source: string): string {
  const match = contents.match(pattern);
  expect(match, `missing Bun version in ${source}`).not.toBeNull();
  return match?.[1] ?? '';
}

describe('Bun version consistency', () => {
  it('keeps runtime, types, tooling, workflows, and documentation aligned', async () => {
    const packageJson = JSON.parse(await readFile(new URL('package.json', root), 'utf8')) as {
      packageManager: string;
      devDependencies: Record<string, string>;
    };
    const version = requiredMatch(
      packageJson.packageManager,
      /^bun@(\d+\.\d+\.\d+)$/u,
      'package.json',
    );

    expect(packageJson.devDependencies['bun-types']).toBe(version);

    const dockerfile = await readFile(new URL('Dockerfile', root), 'utf8');
    expect(
      requiredMatch(dockerfile, /^FROM oven\/bun:(\d+\.\d+\.\d+)-alpine\b/mu, 'Dockerfile'),
    ).toBe(version);

    const workflowsDirectory = new URL('.github/workflows/', root);
    const workflowNames = (await readdir(workflowsDirectory)).filter((name) =>
      /\.ya?ml$/u.test(name),
    );
    const bunWorkflows: string[] = [];

    for (const name of workflowNames) {
      const workflow = await readFile(new URL(name, workflowsDirectory), 'utf8');
      if (!workflow.includes('oven-sh/setup-bun@')) continue;

      bunWorkflows.push(name);
      const configuredVersion = /^\s*bun-version:\s*\$\{\{\s*env\.BUN_VERSION\s*\}\}\s*$/mu.test(
        workflow,
      )
        ? requiredMatch(workflow, /^\s*BUN_VERSION:\s*(\d+\.\d+\.\d+)\s*$/mu, name)
        : requiredMatch(workflow, /^\s*bun-version:\s*(\d+\.\d+\.\d+)\s*$/mu, name);

      expect(configuredVersion, name).toBe(version);
    }

    expect(bunWorkflows.sort()).toEqual(
      ['ci.yml', 'deploy-production.yml', 'publish-image.yml', 'security.yml'].sort(),
    );

    const readme = await readFile(new URL('README.md', root), 'utf8');
    expect(readme).toContain(`Bun ${version}`);
  });
});
