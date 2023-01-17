/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package objects;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import hydra.GsRandom;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Represents an asset held in {@link FastAssetAccount}.
 */
public class FastAsset implements ConfigurableObject, DataSerializable {

  // INSTANTIATORS DO NOT WORK ON GATEWAYS due to bug 35646
  static {
    Instantiator.register(new Instantiator(FastAsset.class, (byte)24) {
      public DataSerializable newInstance() {
        return new FastAsset();
      }
    });
  }

  private static final GsRandom rng = new GsRandom(12); // need determinism

  private int assetId;
  private double value;

  public FastAsset() {
  }

  public void init(int anAssetId) {
    this.assetId = anAssetId;
    this.value = rng.nextDouble(1, FastAssetPrms.getMaxValue());
  }

  /**
   * Makes a copy of this asset.
   */
  public FastAsset copy() {
    FastAsset asset = new FastAsset();
    asset.setAssetId(this.getAssetId());
    asset.setValue(this.getValue());
    return asset;
  }

  /**
   * Returns the id of the asset.
   */
  public int getAssetId(){
    return this.assetId;
  }

  /**
   * Sets the id of the asset.
   */
  public void setAssetId(int i){
    this.assetId = i;
  }

  /**
   * Returns the asset value.
   */
  public double getValue(){
    return this.value;
  }

  /**
   * Sets the asset value.
   */
  public void setValue(double d) {
    this.value = d;
  }

  public int getIndex() {
    return this.assetId;
  }

  public void validate(int index) {
    int encodedIndex = this.getIndex();
    if (encodedIndex != index) {
      String s = "Expected index " + index + ", got " + encodedIndex;
      throw new ObjectValidationException(s);
    }
  }

  public String toString(){
    return "FastAsset [assetId=" + this.assetId + " value=" + this.value + "]";
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof FastAsset) {
      FastAsset asset = (FastAsset)obj;
      return this.assetId == asset.assetId;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return this.assetId;
  }

//------------------------------------------------------------------------------
// DataSerializable

  public void toData(DataOutput out)
  throws IOException {
    out.writeInt(this.assetId);
    out.writeDouble(this.value);
  }
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.assetId = in.readInt();
    this.value = in.readDouble();
  }
}
