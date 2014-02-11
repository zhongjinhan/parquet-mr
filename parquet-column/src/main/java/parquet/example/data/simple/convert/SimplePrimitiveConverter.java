/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.example.data.simple.convert;

import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

class SimplePrimitiveConverter extends PrimitiveConverter {

  private final SimpleGroupConverter parent;
  private final int index;

  SimplePrimitiveConverter(SimpleGroupConverter parent, int index) {
    this.parent = parent;
    this.index = index;
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addBinary(Binary)
   */
  @Override
  public void addBinary(Binary value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addInt96(Binary)
   */
  @Override
  public void addInt96(Binary value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addBoolean(boolean)
   */
  @Override
  public void addBoolean(boolean value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addDouble(double)
   */
  @Override
  public void addDouble(double value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addFloat(float)
   */
  @Override
  public void addFloat(float value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addInt(int)
   */
  @Override
  public void addInt(int value) {
    parent.getCurrentRecord().add(index, value);
  }

  /**
   * {@inheritDoc}
   * @see parquet.io.api.PrimitiveConverter#addLong(long)
   */
  @Override
  public void addLong(long value) {
    parent.getCurrentRecord().add(index, value);
  }

}
