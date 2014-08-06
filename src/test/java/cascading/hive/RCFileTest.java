/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.hive;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import junitx.framework.FileAssert;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/*
 */
public class RCFileTest {

  private FlowConnector connector;

  private String rc, txt, complex_rc;

  @Before
  public void setup() {
    connector = new HadoopFlowConnector(new Properties());
    rc = "src/test/resources/data/test.rc";
    txt = "src/test/resources/data/test.txt";
    complex_rc = "src/test/resources/data/complex.rc";
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    //   FileUtils.deleteDirectory(new File("output"));
  }

  @Test
  public void testRead() throws Exception {
    Lfs input = new Lfs(new RCFile("col1 int, col2 string, col3 string"), rc);
    Pipe pipe = new Pipe("convert");
    Lfs output = new Lfs(new TextDelimited(true, ","), "output/rc_read/", SinkMode.REPLACE);
    Flow flow = connector.connect(input, output, pipe);
    flow.complete();
    FileAssert.assertEquals(new File(txt), new File("output/rc_read/part-00000"));
  }

  @Test
  public void testWrite() throws Exception {
    Lfs input = new Lfs(new TextDelimited(true, ","), txt);
    Pipe pipe = new Pipe("convert");
    Lfs output = new Lfs(new RCFile(new String[]{"col1", "col2", "col3"},
        new String[] {"int","string","string"}), "out/rc_write/", SinkMode.REPLACE);
    Flow flow = connector.connect(input, output, pipe);
    flow.complete();

    TupleEntryIterator it1 = output.openForRead(flow.getFlowProcess());
    TupleEntryIterator it2 = new Lfs(new RCFile("col1 int, col2 string, col3 string"), rc).openForRead(flow.getFlowProcess());
    while(it1.hasNext() && it2.hasNext()) {
      Tuple actual = it1.next().getTuple();
      Tuple expected = it2.next().getTuple();
      assertEquals(expected.getInteger(0), actual.getInteger(0));
      assertEquals(expected.getString(1), actual.getString(1));
      assertEquals(expected.getString(2), actual.getString(2));
    }
  }

  @Test
  public void testWriteComplex() throws Exception {
    final Scheme scheme = new RCFile("col1 int, col2 string, col3 string, col4 array<int>");

    final Lfs input = new Lfs(scheme, complex_rc, SinkMode.REPLACE);
    final Lfs output = new Lfs(scheme, complex_rc + ".test", SinkMode.REPLACE);

    final TupleEntryCollector tec = input.openForWrite(new HadoopFlowProcess());
    tec.add(new Tuple(1, "a", "A", Arrays.asList(1, 2)));
    tec.close();

    final Pipe pipe = new Pipe("convert");
    final Flow flow = connector.connect(input, output, pipe);
    flow.complete();

    final TupleEntryIterator it1 = output.openForRead(new HadoopFlowProcess());
    final TupleEntryIterator it2 = new Lfs(scheme, complex_rc + ".test").openForRead(new HadoopFlowProcess());

    while(it1.hasNext() && it2.hasNext()) {
      final Tuple expected = it1.next().getTuple();
      final Tuple actual = it2.next().getTuple();
      assertNotNull(expected.getInteger(0));
      assertEquals(expected.getInteger(0), actual.getInteger(0));
      assertEquals(expected.getString(1), actual.getString(1));
      assertEquals(expected.getString(2), actual.getString(2));
      assertNotNull(expected.getObject(3));
      assertNotNull(actual.getObject(3));
      assertEquals(expected.getObject(3), actual.getObject(3));
    }
  }

  @Test
  public void testCompress() throws IOException {
    Properties p = new Properties();
    p.put("mapred.output.compress", "true");
    p.put("mapred.output.compression.type", "BLOCK");
    //        GzipCodec needs native lib, otherwise the output can be read.
    //        p.put("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    connector = new HadoopFlowConnector(p);

    Lfs input = new Lfs(new TextDelimited(true, ","), txt);
    Pipe pipe = new Pipe("convert");
    Lfs output = new Lfs(new RCFile(new String[]{"col1", "col2", "col3"},
        new String[] {"int","string","string"}), "output/rc_compress/", SinkMode.REPLACE);
    Flow flow = connector.connect(input, output, pipe);
    flow.complete();
    //compare result with uncompressed ones
    TupleEntryIterator it1 = output.openForRead(flow.getFlowProcess());
    TupleEntryIterator it2 = new Lfs(new RCFile("col1 int, col2 string, col3 string"), rc).openForRead(flow.getFlowProcess());
    while(it1.hasNext() && it2.hasNext()) {
      Tuple actual = it1.next().getTuple();
      Tuple expected = it2.next().getTuple();
      assertEquals(expected.getInteger(0), actual.getInteger(0));
      assertEquals(expected.getString(1), actual.getString(1));
      assertEquals(expected.getString(2), actual.getString(2));
    }
  }
  @Test
  public void testCountBy() throws IOException {
    Lfs input = new Lfs(new RCFile("col1 int, col2 string, col3 string", "0"), rc);
    Pipe pipe = new Pipe("testCountBy");
    pipe = new CountBy(pipe, new Fields("col1"), new Fields("cnt"));
    Lfs output = new Lfs(new TextDelimited(true, ","), "output/rc_count/", SinkMode.REPLACE);
    Flow flow = connector.connect(input, output, pipe);
    flow.complete();
    TupleEntryIterator it = output.openForRead(flow.getFlowProcess());
    Tuple[] expected = new Tuple[] {
        new Tuple("1", "3"),
        new Tuple("2", "3"),
        new Tuple("3", "1"),
        new Tuple("4", "3"),
        new Tuple("5", "3")
    };
    int i = 0;
    while (it.hasNext()) {
      Tuple actual = it.next().getTuple();
      assertEquals(expected[i++], actual);
    }
  }
}
