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
package parquet.column.values.dictionary;

import static parquet.bytes.BytesUtils.readIntLittleEndian;
import static parquet.column.Encoding.PLAIN_DICTIONARY;

import java.io.IOException;

import java.nio.ByteBuffer;
import parquet.column.Dictionary;
import parquet.column.page.DictionaryPage;
import parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

/**
 * a simple implementation of dictionary for plain encoded values
 *
 */
public abstract class PlainValuesDictionary extends Dictionary {

  /**
   * @param dictionaryPage the PLAIN encoded content of the dictionary
   * @throws IOException
   */
  protected PlainValuesDictionary(DictionaryPage dictionaryPage) throws IOException {
    super(dictionaryPage.getEncoding());
    if (dictionaryPage.getEncoding() != PLAIN_DICTIONARY) {
      throw new ParquetDecodingException("Dictionary data encoding type not supported: " + dictionaryPage.getEncoding());
    }
  }

  /**
   * a simple implementation of dictionary for plain encoded binary
   */
  public static class PlainBinaryDictionary extends PlainValuesDictionary {

    private Binary[] binaryDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainBinaryDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      binaryDictionaryContent = new Binary[dictionaryPage.getDictionarySize()];
      // dictionary values are stored in order: size (4 bytes LE) followed by {size} bytes
      int offset = 0;
      for (int i = 0; i < binaryDictionaryContent.length; i++) {
        int length = readIntLittleEndian(dictionaryBytes, offset);
        // read the length
        offset += 4;
        // wrap the content in a binary
        binaryDictionaryContent[i] = Binary.fromByteArray(dictionaryBytes, offset, length);
        // increment to the next value
        offset += length;
      }
    }

    @Override
    public Binary decodeToBinary(int id) {
      return binaryDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainBinaryDictionary {\n");
      for (int i = 0; i < binaryDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(binaryDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return binaryDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded fixed length arrays
   */
  public static class PlainFixedLenByteArrayValuesDictionary extends PlainValuesDictionary {

    private Binary[] fixedDictionaryContent = null;
    private final int length;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainFixedLenByteArrayValuesDictionary(DictionaryPage dictionaryPage, int length) throws IOException {
      super(dictionaryPage);
      this.length = length;
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      fixedDictionaryContent = new Binary[dictionaryPage.getDictionarySize()];
      int offset = 0;
      for (int i = 0; i < fixedDictionaryContent.length; i++) {
        // wrap the content in a Binary
        fixedDictionaryContent[i] = Binary.fromByteArray(
            dictionaryBytes, offset, length);
        // increment to the next value
        offset += length;
      }
    }

    @Override
    public Binary decodeToBinary(int id) {
      return fixedDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainFixedLenByteArrayValuesDictionary {\n");
      for (int i = 0; i < fixedDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(fixedDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return fixedDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded long values
   */
  public static class PlainLongDictionary extends PlainValuesDictionary {

    private long[] longDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainLongDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      longDictionaryContent = new long[dictionaryPage.getDictionarySize()];
      LongPlainValuesReader longReader = new LongPlainValuesReader();
      longReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < longDictionaryContent.length; i++) {
        longDictionaryContent[i] = longReader.readLong();
      }
    }

    @Override
    public long decodeToLong(int id) {
      return longDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainLongDictionary {\n");
      for (int i = 0; i < longDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(longDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return longDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded double values
   */
  public static class PlainDoubleDictionary extends PlainValuesDictionary {

    private double[] doubleDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainDoubleDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      doubleDictionaryContent = new double[dictionaryPage.getDictionarySize()];
      DoublePlainValuesReader doubleReader = new DoublePlainValuesReader();
      doubleReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        doubleDictionaryContent[i] = doubleReader.readDouble();
      }
    }

    @Override
    public double decodeToDouble(int id) {
      return doubleDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainDoubleDictionary {\n");
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(doubleDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return doubleDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded integer values
   */
  public static class PlainIntegerDictionary extends PlainValuesDictionary {

    private int[] intDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainIntegerDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      intDictionaryContent = new int[dictionaryPage.getDictionarySize()];
      IntegerPlainValuesReader intReader = new IntegerPlainValuesReader();
      intReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < intDictionaryContent.length; i++) {
        intDictionaryContent[i] = intReader.readInteger();
      }
    }

    @Override
    public int decodeToInt(int id) {
      return intDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainIntegerDictionary {\n");
      for (int i = 0; i < intDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(intDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return intDictionaryContent.length - 1;
    }

  }

  /**
   * a simple implementation of dictionary for plain encoded float values
   */
  public static class PlainFloatDictionary extends PlainValuesDictionary {

    private float[] floatDictionaryContent = null;

    /**
     * @param dictionaryPage
     * @throws IOException
     */
    public PlainFloatDictionary(DictionaryPage dictionaryPage) throws IOException {
      super(dictionaryPage);
      final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
      floatDictionaryContent = new float[dictionaryPage.getDictionarySize()];
      FloatPlainValuesReader floatReader = new FloatPlainValuesReader();
      floatReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        floatDictionaryContent[i] = floatReader.readFloat();
      }
    }

    @Override
    public float decodeToFloat(int id) {
      return floatDictionaryContent[id];
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("PlainFloatDictionary {\n");
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(floatDictionaryContent[i]).append("\n");
      }
      return sb.append("}").toString();
    }

    @Override
    public int getMaxId() {
      return floatDictionaryContent.length - 1;
    }

  }

}
