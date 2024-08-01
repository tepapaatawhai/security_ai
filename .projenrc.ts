import { awscdk } from 'projen';
const project = new awscdk.AwsCdkConstructLibrary({
  author: 'afrazeradm',
  authorAddress: 'afrazeradm@doc.govt.nz',
  cdkVersion: '2.1.0',
  defaultReleaseBranch: 'main',
  jsiiVersion: '~5.4.0',
  name: 'security_ai',
  projenrcTs: true,
  repositoryUrl: 'https://git.us-west-2.github.source.3p.codecatalyst.aws/v1/depcon-codecatalyst/security_ai/security_ai',

  // deps: [],                /* Runtime dependencies of this module. */
  // description: undefined,  /* The description is just a string that helps people understand the purpose of the package. */
  // devDeps: [],             /* Build dependencies for this module. */
  // packageName: undefined,  /* The "name" in package.json. */
});
project.synth();