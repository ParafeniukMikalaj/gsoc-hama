package org.apache.hama.examples.linearalgebra.converters;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.linearalgebra.formats.VectorFormat;

/**
 * This class would be used for any conversions
 * between different vector formats. If user want's
 * to add custom converter for Format1 to Format2
 * he must register key-value pair in Hama's configuration
 * in such format:
 * key: linearalgebra.vector.converter.Format1-Format2
 * value: full class name of converter.
 */
public class HamaVectorConverter implements VectorConverter<VectorFormat, VectorFormat> {

  private HamaConfiguration conf;
  private VectorConverter<VectorFormat, VectorFormat> defaultConverter;
  private HashMap<String, VectorConverter<VectorFormat, VectorFormat>> searchedConverters;
  private static Log LOG = LogFactory.getLog(HamaVectorConverter.class);

  @SuppressWarnings("unchecked")
  public HamaVectorConverter() {
    conf = new HamaConfiguration();
    String defaultConverterName = conf
        .get("linearalgebra.vector.converter.default");
    try {
      Class<?> defaultConverterClass = Class.forName(defaultConverterName);
      defaultConverter = (VectorConverter<VectorFormat, VectorFormat>) defaultConverterClass
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

  @SuppressWarnings("unchecked")
  @Override
  public void convert(VectorFormat f, VectorFormat t) {
    // Maybe it will be better to use full class names?
    VectorConverter<VectorFormat, VectorFormat> converter;
    String fromClassName = f.getClass().getSimpleName();
    String toClassName = t.getClass().getSimpleName();
    StringBuilder confStringBuilder = new StringBuilder();
    String confString = confStringBuilder
        .append("linearalgebra.vector.converter.").append(fromClassName)
        .append("-").append(toClassName).toString();
    if (!searchedConverters.containsKey(confString)) {
      String converterClassName = conf.get(confString.toString(), null);
      Class<?> converterClass;
      try {
        if (converterClassName != null) {
          converterClass = Class.forName(converterClassName);
          converter = (VectorConverter<VectorFormat, VectorFormat>) converterClass
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

