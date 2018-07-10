package cc.shanruifeng.functions.array;

import cc.shanruifeng.functions.fastuitl.ints.IntArrays;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;

import static cc.shanruifeng.functions.utils.ArrayUtils.IntArrayCompare;

@Description(name = "array_continuous_reduction"
        , value = "_FUNC_(array) - returns the array without continuous duplicates."
        , extended = "Example:\n > select _FUNC_(array) from src;")
public class UDFArrayContinuousReduction extends GenericUDF {
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private transient ListObjectInspector leftArrayOI;
    private transient ObjectInspector leftArrayElementOI;

    private transient ArrayList<Object> result = new ArrayList<Object>();
    private transient Converter converter;

    public UDFArrayContinuousReduction() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if two arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentLengthException(
                    "The function array_continuous_reduction(array) takes exactly " + ARG_COUNT + "arguments.");
        }

        // Check if two argument is of category LIST
        for (int i = 0; i < ARG_COUNT; i++) {
            if (!arguments[i].getCategory().equals(ObjectInspector.Category.LIST)) {
                throw new UDFArgumentTypeException(i,
                        "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                                + "expected at function array_continuous_reduction, but "
                                + "\"" + arguments[i].getTypeName() + "\" "
                                + "is found");
            }
        }

        leftArrayOI = (ListObjectInspector) arguments[0];

        leftArrayElementOI = leftArrayOI.getListElementObjectInspector();

        // Check if the comparison is supported for this type
        if (!ObjectInspectorUtils.compareSupported(leftArrayElementOI)) {
            throw new UDFArgumentException("The function array_continuous_reduction"
                    + " does not support comparison for "
                    + "\"" + leftArrayElementOI.getTypeName() + "\""
                    + " types");
        }

        converter = ObjectInspectorConverters.getConverter(leftArrayElementOI, leftArrayElementOI);

        return ObjectInspectorFactory.getStandardListObjectInspector(leftArrayElementOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object leftArray = arguments[0].get();

        int leftArrayLength = leftArrayOI.getListLength(leftArray);

        // Check if array is null or empty
        if (leftArray == null || leftArrayLength < 0) {
            return null;
        }

        if (leftArrayLength == 0) {
            return leftArray;
        }

        result.clear();
        int leftCurrentPosition = 0;
        int leftBasePosition;

        while (leftCurrentPosition < leftArrayLength) {
            leftBasePosition = leftCurrentPosition;
            Object leftArrayElement = leftArrayOI.getListElement(leftArray, leftCurrentPosition);
	    result.add(converter.convert(leftArrayElement));

                if (leftCurrentPosition < leftArrayLength) {
                    Object leftArrayElementTmp1 = leftArrayOI.getListElement(leftArray, leftBasePosition);
                    Object leftArrayElementTmp2 = leftArrayOI.getListElement(leftArray, leftCurrentPosition);
                    while (leftCurrentPosition < leftArrayLength && ObjectInspectorUtils.compare(leftArrayElementTmp1, leftArrayElementOI, leftArrayElementTmp2, leftArrayElementOI) == 0) {
                        leftCurrentPosition++;
                        leftArrayElementTmp2 = leftArrayOI.getListElement(leftArray, leftCurrentPosition);
                    }
                }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_continuous_reduction(" + strings[0] + ")";
    }
}
