package ca.uwaterloo.cs451.a3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;


import io.bespin.java.mapreduce.pagerank.PageRankNode;
import ca.uwaterloo.cs451.a4.PageRankNodeArray;
public class bytetest {

    public static void main(String[] args) throws IOException {
        // ByteArrayOutputStream BAOS = new ByteArrayOutputStream();
        // DataOutputStream DOS= new DataOutputStream(BAOS); 

        
        // for (int i = 10; i <= 20; i++) {
        //     WritableUtils.writeVInt(DOS, i);

        // }
        // DOS.flush();

        // BAOS.reset();
        // for (int i = 1; i <= 5; i++) {
        //     WritableUtils.writeVInt(DOS, i);

        // }
        // DataInputStream INST = new DataInputStream(new ByteArrayInputStream(BAOS.toByteArray()));

        // while (INST.available() > 0){
        //     System.out.println(WritableUtils.readVInt(INST));
        // }
        // // byte[] b_array=BAOS.toByteArray();
        // // for(int i=0; i<b_array.length; i++) {
        // //     System.out.println((int)b_array[i]);

        // // }
        // System.out.println(BAOS.toString());
        // System.out.println("TEST CLASS");


        // String test="367,249,145";
        // String[] arr = test.trim().split(",");
        // for (int i = 0; i<arr.length; i++){
        //     System.out.println(arr[i]);
        // }

        PageRankNode pr=new PageRankNode();
        pr.setType(PageRankNode.Type.Complete);
        pr.setNodeId(1);
        System.out.println(pr.getNodeId());

        PageRankNodeArray pra=new PageRankNodeArray();
        pra.setNodeId(5);
        // pra.setPageRankArray(Float.valueOf((float)1.25));
        // pra.setPageRankArray(Float.valueOf((float)1));
        System.out .println(pra.toString());
        
    }
}
