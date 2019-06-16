#!/usr/bin/env node
import 'source-map-support/register';
import cdk = require('@aws-cdk/cdk');
import { AnimlStack } from '../lib/animl-stack';

const app = new cdk.App();
new AnimlStack(app, 'AnimlStack');
