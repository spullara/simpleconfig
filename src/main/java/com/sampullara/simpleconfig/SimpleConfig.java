package com.sampullara.simpleconfig;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Utterly trivial configuration system leveraging SimpleDB.  Typically system properties or environment variables
 * are used for populating the very sensitive access and secret key data.  Most usages except for testing should
 * likely use the default constructor. Don't use NULL_CHAR in your key or as a value.
 * <p/>
 * Keeps a local copy of the config just in case AWS is unreachable.
 * <p/>
 * User: sam
 * Date: May 9, 2010
 * Time: 7:35:39 PM
 */
public class SimpleConfig {
  private String env;
  private AmazonSimpleDBClient sdb;
  private static final String NULL_CHAR = "\u0000";
  private static ExecutorService es = Executors.newFixedThreadPool(1);

  public SimpleConfig() {
    this(new ClientConfiguration());
  }

  public SimpleConfig(ClientConfiguration cc) {
    Properties p = System.getProperties();
    String env = System.getenv("env");
    this.env = p.getProperty("env", env == null ? "default_config" : env);
    BasicAWSCredentials awsCredentials = new BasicAWSCredentials(
        p.getProperty("accessKey", System.getenv("AWS_ACCESS_KEY_ID")),
        p.getProperty("secretKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    );
    init(awsCredentials, cc);
  }

  public SimpleConfig(String accessKey, String secretKey, String env) {
    this(accessKey, secretKey, env, new ClientConfiguration());
  }

  public SimpleConfig(String accessKey, String secretKey, String env, ClientConfiguration cc) {
    this.env = env;
    BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
    init(awsCredentials, cc);
  }

  private void init(BasicAWSCredentials awsCredentials, ClientConfiguration cc) {
    sdb = new AmazonSimpleDBClient(awsCredentials, cc);
    ListDomainsResult result = sdb.listDomains();
    if (!result.getDomainNames().contains(env)) {
      sdb.createDomain(new CreateDomainRequest(env));
    }
  }

  private Map<String, String> cache = new ConcurrentHashMap<String, String>();

  public void refresh() {
    List<String> conkeys = new ArrayList<String>(cache.keySet());
    cache.clear();
    for (String conkey : conkeys) {
      String[] pair = conkey.split(NULL_CHAR);
      get(pair[0], pair[1]);
    }
  }

  public void set(final String config, String key, String value) {
    ReplaceableAttribute attribute = new ReplaceableAttribute();
    attribute.setName(key);
    attribute.setValue(value);
    attribute.setReplace(true);
    sdb.putAttributes(new PutAttributesRequest(env, config, Arrays.asList(attribute)));
    put(config, key, value);
  }

  private void put(final String config, String key, String value) {
    String conkey = conkey(config, key);
    cache.put(conkey, value);
    es.submit(new Runnable() {
      @Override
      public void run() {
        File file = new File(config + ".properties");
        Properties p = new Properties();
        for (Map.Entry<String, String> entry : cache.entrySet()) {
          String[] conkey = entry.getKey().split(NULL_CHAR);
          p.put(conkey[1], entry.getValue());
        }
        try {
          p.store(new FileWriter(file), "Client-side property cache");
        } catch (IOException e) {
          // ignore
        }
      }
    });
  }

  private String conkey(String config, String key) {
    return config + NULL_CHAR + key;
  }

  public String get(String config, String key) {
    String conkey = conkey(config, key);
    String value = cache.get(conkey);
    if (value == null) {
      GetAttributesRequest req = new GetAttributesRequest(env, config);
      req.setAttributeNames(Arrays.asList(key));
      try {
        GetAttributesResult result = sdb.getAttributes(req);
        for (Attribute attribute : result.getAttributes()) {
          if (attribute.getName().equals(key)) {
            value = attribute.getValue();
          }
        }
      } catch (Exception e) {
        // Failed to get from Amazon and not in cache. Grab the local copy.
        synchronized (this) {
          // Check to see if the cache has been populated in another thread
          if ((value = cache.get(conkey)) == null) {
            File file = new File(config + ".properties");
            Properties p = new Properties();
            try {
              p.load(new FileReader(file));
            } catch (IOException e1) {
              // and failed to get local copy
              return null;
            }
            for (Map.Entry entry : p.entrySet()) {
              Object o = entry.getKey();
              Object v = entry.getValue();
              put(config, o == null ? null : o.toString(), v == null ? null : v.toString());
            }
            value = cache.get(conkey);
          }
        }
      }
      // Negative cache
      if (value == null) {
        value = NULL_CHAR;
      }
      cache.put(conkey, value);
    }
    return value.equals(NULL_CHAR) ? null : value;
  }
}
