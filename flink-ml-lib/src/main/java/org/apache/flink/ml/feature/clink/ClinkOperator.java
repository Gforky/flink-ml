package org.apache.flink.ml.feature.clink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

public class ClinkOperator {
    private static ClinkWrapper CLINK_INST;
    private static Pointer REMOTE_PATH;
    private static Pointer LOCAL_PATH;

    public ClinkOperator(String clinkSoPath, String remoteConfPath, String localConfPath) {
        CLINK_INST = getClinkInst(clinkSoPath);
        REMOTE_PATH = getRemotePath(remoteConfPath);
        LOCAL_PATH = getLocalPath(localConfPath);
    }

    private static String featureExtract(String[] inputCols, Row input) {
        ArrayList<String> strBuffer = new ArrayList<>();
        for (int i = 0; i < inputCols.length; i++) {
            String strFeatVal = input.getField(inputCols[i]).toString();
            strBuffer.add(strFeatVal);
        }
        String inputStr = StringUtils.join(strBuffer, ",");
        Pointer pInput = new Memory((inputStr.length() + 1) * Native.WCHAR_SIZE);
        pInput.setString(0, inputStr);
        PointerByReference ptrRef = new PointerByReference(Pointer.NULL);
        int res = CLINK_INST.FeatureExtractOffline(REMOTE_PATH, LOCAL_PATH, pInput, ptrRef);
        if (res != 0) {
            return null;
        }
        final Pointer p = ptrRef.getValue();
        // extract the null-terminated string from the Pointer
        final String val = p.getString(0);
        CLINK_INST.FeatureOfflineCleanUp(p);
        return val;
    }

    public static ClinkWrapper getClinkInst(String clinkSoPath) {
        return Native.load(clinkSoPath, ClinkWrapper.class);
    };

    public static Pointer getRemotePath(String remotePath) {

        Pointer pRemotePath = new Memory((remotePath.length() + 1) * Native.WCHAR_SIZE);
        pRemotePath.setString(0, remotePath);

        return pRemotePath;
    }

    public static Pointer getLocalPath(String localPath) {

        Pointer pPath = new Memory((localPath.length() + 1) * Native.WCHAR_SIZE);
        pPath.setString(0, localPath);

        return pPath;
    }

    public Table[] clinkTransform(Table modelTable, String[] inputCols, Table... inputs) {

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) modelTable).getTableEnvironment();
        DataStream<Row> input = tEnv.toDataStream(inputs[0]);
        DataStream<Row> output = input.map(x -> Row.of(featureExtract(inputCols, x)));

        Table outputTable = tEnv.fromDataStream(output);

        return new Table[] {outputTable};
    }
}
