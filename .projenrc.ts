// https://cloud-networking-as-code.com/?p=319

import { awscdk } from 'projen';
const project = new awscdk.AwsCdkConstructLibrary({
  author: 'Andrew Frazer',
  authorAddress: 'andrew.frazer@raindancers.cloud',
  cdkVersion: '2.150.0',
  defaultReleaseBranch: 'main',
  jsiiVersion: '~5.4.0',
  name: 'security_ai',
  projenrcTs: true,
  repositoryUrl: 'https://github.com/tepapaatawhai/security_ai',
  keywords: [
    'securitylake',
    'aws-cdk',
    'bedrock',
  ],
  license: 'Apache-2.0',
  publishToPypi: {
    distName: 'flintstones',
    module: 'securitylake',
  },
  stability: 'experimental',
});

project.addGitIgnore('!/projectAssets/**');

project.synth();