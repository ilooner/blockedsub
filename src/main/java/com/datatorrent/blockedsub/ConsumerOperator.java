/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.blockedsub;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConsumerOperator extends BaseOperator implements Partitioner<ConsumerOperator>
{
  private long multiplier;
  private int partitionCount = 8;
  public final transient DefaultInputPort<Double> input = new DefaultInputPort<Double>() {

    @Override
    public void process(Double t)
    {
      //do nothing
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    try {
      Thread.sleep(1000 * multiplier);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Collection<Partition<ConsumerOperator>> definePartitions(Collection<Partition<ConsumerOperator>> clctn, PartitioningContext pc)
  {
    Kryo kryo = new Kryo();
    ConsumerOperator consumer = clctn.iterator().next().getPartitionedInstance();
    Collection<Partition<ConsumerOperator>> newPartitions = Lists.newArrayList();

    for (int partitionCount = 0; partitionCount < getPartitionCount(); partitionCount++) {
      ConsumerOperator cloned = clone(kryo, consumer);
      cloned.setMultiplier((long) (partitionCount + 1));
      newPartitions.add(new DefaultPartition(cloned));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<ConsumerOperator>> map)
  {
  }

  public static <T> T clone(Kryo kryo, T src)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, src);
    output.close();
    Input input = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)src.getClass();
    return kryo.readObject(input, clazz);
  }

  /**
   * @return the multiplier
   */
  public long getMultiplier()
  {
    return multiplier;
  }

  /**
   * @param multiplier the multiplier to set
   */
  public void setMultiplier(long multiplier)
  {
    this.multiplier = multiplier;
  }

  /**
   * @return the partitionCount
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * @param partitionCount the partitionCount to set
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }
}
