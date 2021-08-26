import { expect as expectCDK, matchTemplate, MatchStyle } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as BlogGlueWorkFlow from '../lib/blog-glue_work_flow-stack';
import { BlogGlueWorkFlowStack } from '../lib/blog-glue_work_flow-stack';

test('Empty Stack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new BlogGlueWorkFlow.BlogGlueWorkFlowStack(app, 'MyTestStack',{});
    // THEN
    expectCDK(stack).to(matchTemplate({
      "Resources": {}
    }, MatchStyle.EXACT))
});
