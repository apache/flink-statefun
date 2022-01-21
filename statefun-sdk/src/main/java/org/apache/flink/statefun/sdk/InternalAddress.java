package org.apache.flink.statefun.sdk;

public class InternalAddress {
  public Address address;
  public FunctionType internalType;

  public InternalAddress(Address address, FunctionType internalType){
    this.address = address;
    this.internalType = internalType;
  }

  public Address toAddress(){
    return address;
  }

  @Override
  public String toString(){
    return String.format("InternalAddress <%s, %s>", address.toString(), (internalType==null? "null":internalType.toString()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InternalAddress internalAddress = (InternalAddress) o;
    return address.equals(internalAddress.address) &&
            ((internalType ==null && internalAddress.internalType==null)
                    || (internalType!=null && internalAddress.internalType!=null&& internalType.equals(internalAddress.internalType)));
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = 37 * hash + address.hashCode();
    hash = 37 * hash + (internalType==null?0:internalType.hashCode());
    return hash;
  }
}
