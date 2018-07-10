package cc.shanruifeng.functions.array;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class UDFArrayContinuousReductionTest {
    @Test
    public void testArrayContinuousReduction() throws Exception {
        UDFArrayContinuousReduction udf = new UDFArrayContinuousReduction();

        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        ObjectInspector[] arguments = {arrayOI,};
        udf.initialize(arguments);

        List<String> array = ImmutableList.of("a", "b", "c");
        DeferredObject arrayObj = new DeferredJavaObject(array);
        DeferredObject[] args = {arrayObj,};
        ArrayList output = (ArrayList) udf.evaluate(args);
        assertEquals("array_continuous_reduction() test", array, output);

        // Try with null args
        DeferredObject[] nullArgs = { new DeferredJavaObject(null), };
        output = (ArrayList) udf.evaluate(nullArgs);
        assertEquals("array_continuous_reduction() test", null, output);

        array = ImmutableList.of("1", "2", "2", "3", "4", "4", "1");
        arrayObj = new DeferredJavaObject(array);
        args[0] = arrayObj;
        output = (ArrayList) udf.evaluate(args);
        assertEquals("array_continuous_reduction() test", ImmutableList.of("1", "2", "3", "4", "1"), output);
    }
}
