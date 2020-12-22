package ca.uwaterloo.cs451.a4;

import io.bespin.java.mapreduce.pagerank.PageRankNode;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.array.ArrayListOfFloatsWritable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Representation of a graph node of PageRank with defined sources.
 * Extend from PageRankNode.
 * 
 * @author Ting Pan
 */

public class PageRankNodeArray extends PageRankNode{

    // define a new filed pageRankArray
    private ArrayListOfFloatsWritable pageRankArray;

    // constructor
    public PageRankNodeArray(){
        super();
    }

    // set and get method for the field pageRankArray
    public void setPageRankArray (ArrayListOfFloatsWritable rank) {
        this.pageRankArray=rank;
    }
    public ArrayListOfFloatsWritable getPageRankArray () {
        return this.pageRankArray;
    }


    @Override
    public String toString() {
      return String.format("{%d %s %s}", this.getNodeId(), (this.pageRankArray==null ? "[]" : this.pageRankArray.toString() ),
          (this.getAdjacencyList() == null ? "[]": this.getAdjacencyList().toString(10)));
    }


  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   * @throws IOException if any exception is encountered during object deserialization
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int b = in.readByte();
    this.setType(PageRankNode.Type.values()[b]);
    this.setNodeId(in.readInt());

    if (this.getType().equals(Type.Mass)) {
      ArrayListOfFloatsWritable pra = new ArrayListOfFloatsWritable();
      pra.readFields(in);
      this.pageRankArray=pra;
      
      return;
    }

    if (this.getType().equals(Type.Complete)) {
      ArrayListOfFloatsWritable pra = new ArrayListOfFloatsWritable();
      pra.readFields(in);
      this.pageRankArray=pra;
    }

    ArrayListOfIntsWritable adjacencyList = new ArrayListOfIntsWritable();
    adjacencyList.readFields(in);
    this.setAdjacencyList(adjacencyList);
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   * @throws IOException if any exception is encountered during object serialization
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(this.getType().val);
    out.writeInt(this.getNodeId());

 
    if (this.getType().equals(Type.Mass)) {
        this.pageRankArray.write(out);
      return;
    }

    if (this.getType().equals(Type.Complete)) {
        this.pageRankArray.write(out);
    }

    this.getAdjacencyList().write(out);
  }


  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException if any exception is encountered during object deserialization
   */
  public static PageRankNodeArray create(DataInput in) throws IOException {
    PageRankNodeArray m = new PageRankNodeArray();
    m.readFields(in);

    return m;
  }


  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException if any exception is encountered during object deserialization
   */
  public static PageRankNodeArray create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
