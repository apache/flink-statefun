package org.apache.flink.statefun.e2e.smoke;

final class Ids {
  private final String[] cache;

  public Ids(int maxIds) {
    this.cache = createIds(maxIds);
  }

  public String idOf(int address) {
    return cache[address];
  }

  private static String[] createIds(int maxIds) {
    String[] ids = new String[maxIds];
    for (int i = 0; i < maxIds; i++) {
      ids[i] = Integer.toString(i);
    }
    return ids;
  }
}
