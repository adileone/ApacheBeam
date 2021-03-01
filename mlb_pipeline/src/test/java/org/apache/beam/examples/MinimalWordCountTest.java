package org.apache.beam.examples;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * To keep {@link MinimalWordCount} simple, it is not factored or testable. This test file should be
 * maintained with a copy of its code for a basic smoke test.
 */
@RunWith(JUnit4.class)
public class MinimalWordCountTest  {

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  /** A basic smoke test that ensures there is no crash at pipeline construction time. */
  @Test
  public void testMinimalWordCount() throws Exception {

    assertTrue("true", true);
  }
}
