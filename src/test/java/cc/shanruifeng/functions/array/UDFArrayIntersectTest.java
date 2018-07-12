package cc.shanruifeng.functions.array;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.junit.Assert.*;


public class UDFArrayIntersectTest {
    @Test
    public void testArrayIntersect() throws Exception {
        UDFArrayIntersect udf = new UDFArrayIntersect();

        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        ObjectInspector[] arguments = {arrayOI, arrayOI};

        udf.initialize(arguments);
        List<String> array = ImmutableList.of("a", "b", "c");
        DeferredObject arrayObj = new DeferredJavaObject(array);
        DeferredObject[] args = {arrayObj, arrayObj};
        List output = (List) udf.evaluate(args);

        assertEquals("array_intersect() test", array, output);

        // Try with null args
        DeferredObject[] nullArgs = { new DeferredJavaObject(null), new DeferredJavaObject(null) };
        output = (List) udf.evaluate(nullArgs);
        assertEquals("array_intersect() test", null, output);

        // Try with example args
        ArrayList<String> arrayA = new ArrayList<String>();
        arrayA.add("16");
        arrayA.add("12");
        arrayA.add("18");
        arrayA.add("9");
        arrayA.add(null);
        ArrayList<String> arrayB = new ArrayList<String>();
        arrayB.add("14");
        arrayB.add("9");
        arrayB.add("6");
        arrayB.add("18");
        arrayB.add(null);
        ArrayList<String> expectedAB = new ArrayList<String>();
        expectedAB.add(null);
        expectedAB.add("18");
        expectedAB.add("9");
        DeferredObject[] exampleArgs = { new DeferredJavaObject(arrayA), new DeferredJavaObject(arrayB) };
        output = (List) udf.evaluate(exampleArgs);
        assertEquals("array_intersect() test", expectedAB, output);

        // Edge Case 1. Data Missing.
        arrayB.add("18");
        DeferredObject[] edgeArgs = { new DeferredJavaObject(arrayA), new DeferredJavaObject(arrayB) };
        output = (List) udf.evaluate(edgeArgs);
        assertEquals("array_intersect() test", expectedAB, output);
         
        // Edge Case 2. Array IndexOutOfBoundsException.
        ArrayList<String> arrayC = new ArrayList<String>();
        for (int i = 0; i < /* UDFArrayIntersect.INITIAL_SIZE */ 128; i++) {
            arrayC.add(Integer.toString(i));
        }
        Collections.sort(arrayC);
        ArrayList<String> arrayD = (ArrayList<String>)arrayC.clone();
        arrayD.add(arrayD.get(arrayD.size() - 1));

        DeferredObject[] edge2Args = { new DeferredJavaObject(arrayC), new DeferredJavaObject(arrayD) };
        output = (List) udf.evaluate(edge2Args);
        assertEquals("array_intersect() test", arrayC, output);
    }
}
