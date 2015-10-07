/**
 * Put your copyright and license info here.
 */
package com.datatorrent.blockedsub;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @Override
  public void emitTuples()
  {
    int count = 0;
    
    if (count++ < 1000) {
      out.emit(Math.random());
    }
  }
}
