package com.sampullara.simpleconfig;

import junit.framework.TestCase;

/**
 * Test various cases
 * <p/>
 * User: sam
 * Date: May 9, 2010
 * Time: 8:14:31 PM
 */
public class SimpleConfigTest extends TestCase {
  private static final String CONFIG = SimpleConfigTest.class.getName();

  public void testTrivial() {
    SimpleConfig sc = new SimpleConfig(); // List Domains, possibly Create Domain
    sc.set(CONFIG, "testkey", "testvalue"); // Put
    assertEquals("testvalue", sc.get(CONFIG, "testkey")); // Get from cache
    sc.refresh(); // Purge and refresh cache, Get
    assertEquals("testvalue", sc.get(CONFIG, "testkey")); // Get from cache
    assertEquals(null, sc.get(CONFIG, "testkey-notpresent")); // Get fails
    assertEquals(null, sc.get(CONFIG, "testkey-notpresent")); // Get from cache
  }
}
