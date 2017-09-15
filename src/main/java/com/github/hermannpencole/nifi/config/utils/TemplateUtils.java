package com.github.hermannpencole.nifi.config.utils;

import com.github.hermannpencole.nifi.swagger.client.model.TemplateDTO;
import org.apache.commons.io.IOUtils;

import javax.xml.bind.*;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stream.StreamSource;
import java.io.*;

public class TemplateUtils {

    public static final String TMP_FILE_PREFIX = "nifi-template-";
    public static final String TMP_FILE_SUFFIX = ".xml";

    public static File storeTmpFile(final TemplateDTO template) throws IOException, JAXBException, XMLStreamException {
        File file = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_SUFFIX);
        try (
                InputStream in = new ByteArrayInputStream(serialize(template));
                OutputStream out = new FileOutputStream(file)
        ) {
            IOUtils.copy(in, out);
        }
        return file;
    }

    public static byte[] serialize(final TemplateDTO dto) throws JAXBException, XMLStreamException, IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final BufferedOutputStream bos = new BufferedOutputStream(baos);

        // using jaxb for serialization because of differences in behaviour between jaxb (used by NiFi) and jackson
        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Marshaller marshaller = context.createMarshaller();
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        marshaller.marshal(dto, xmlof.createXMLStreamWriter(bos));

        bos.flush();
        return baos.toByteArray();
    }

    public static TemplateDTO deserialize(InputStream in) throws JAXBException {
        // using jaxb for deserialization because of differences in behaviour between jaxb (used by NiFi) and jackson
        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        JAXBElement<TemplateDTO> element = unmarshaller.unmarshal(new StreamSource(in), TemplateDTO.class);
        return element.getValue();
    }

    public static TemplateDTO deserialize(File file) throws IOException, JAXBException {
        try (InputStream in = new FileInputStream(file)) {
            return deserialize(in);
        }
    }

}
