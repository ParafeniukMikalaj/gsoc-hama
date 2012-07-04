package org.apache.hama.examples.linearalgebra.converters;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.linearalgebra.formats.Matrix;

/**
 * This class would be used for any conversions between different matrix
 * formats. If user want's to add custom converter for Format1 to Format2 he
 * must register key-value pair in Hama's configuration in such format: key:
 * linearalgebra.matrix.converter.Format1-Format2 value: full class name of
 * converter.
 */
public class HamaMatrixConverter implements MatrixConverter<Matrix, Matrix> {

  private HamaConfiguration conf;
  private MatrixConverter<Matrix, Matrix> defaultConverter;
  private HashMap<String, MatrixConverter<Matrix, Matrix>> searchedConverters;
  private static Log LOG = LogFactory.getLog(HamaMatrixConverter.class);

  @SuppressWarnings("unchecked")
  public HamaMatrixConverter() {
    conf = new HamaConfiguration();
    String defaultConverterName = conf
        .get("linearalgebra.matrix.converter.default");
    try {
      Class<?> defaultConverterClass = Class.forName(defaultConverterName);
      defaultConverter = (MatrixConverter<Matrix, Matrix>) defaultConverterClass
          .newInstance();
    } catch (ClassNotFoundException e) {
      LOG.error("ERROR in creation default converter. Class not found.");
      e.printStackTrace();
    } catch (InstantiationException e) {
      LOG.error("ERROR in creation default converter. Instantiation error.");
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      LOG.error("ERROR in creation default converter. Illegal access exception.");
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public void convert(Matrix f, Matrix t) {
    // Maybe it will be better to use full class names?
    MatrixConverter<Matrix, Matrix> converter;
    String fromClassName = f.getClass().getSimpleName();
    String toClassName = t.getClass().getSimpleName();
    StringBuilder confStringBuilder = new StringBuilder();
    String confString = confStringBuilder
        .append("linearalgebra.matrix.converter.").append(fromClassName)
        .append("-").append(toClassName).toString();
    if (!searchedConverters.containsKey(confString)) {
      String converterClassName = conf.get(confString.toString(), null);
      Class<?> converterClass;
      try {
        if (converterClassName != null) {
          converterClass = Class.forName(converterClassName);
          converter = (MatrixConverter<Matrix, Matrix>) converterClass
              .newInstance();
        } else {
          converter = defaultConverter;
        }
        searchedConverters.put(confString, converter);
        converter.convert(f, t);
      } catch (ClassNotFoundException e) {
        LOG.error("ERROR in creation default converter. Class not found.");
        e.printStackTrace();
      } catch (InstantiationException e) {
        LOG.error("ERROR in creation default converter. Instantiation error.");
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        LOG.error("ERROR in creation default converter. Illegal access exception.");
        e.printStackTrace();
      }

    } else {
      converter = searchedConverters.get(confString);
      converter.convert(f, t);
    }
  }

}
