package com.github.hermannpencole.nifi.config.utils;

import com.github.hermannpencole.nifi.swagger.client.model.TemplateDTO;
import org.apache.commons.io.IOUtils;

import javax.xml.bind.*;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stream.StreamSource;
import java.io.*;

/**
 * Utility class providing helper methods for deserializing and serializing templates.
 */
public class TemplateUtils {

    public static final String TMP_FILE_PREFIX = "nifi-template-";
    public static final String TMP_FILE_SUFFIX = ".xml";

    /**
     * Serialize and store template in temporary file.
     * @param template template DTO
     * @return temporary file handle
     * @throws IOException
     * @throws JAXBException
     * @throws XMLStreamException
     */
    public static File serializeAndStoreTmpFile(final TemplateDTO template) throws IOException, JAXBException, XMLStreamException {
        File file = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_SUFFIX);
        try (
                InputStream in = new ByteArrayInputStream(serialize(template));
                OutputStream out = new FileOutputStream(file)
        ) {
            IOUtils.copy(in, out);
        }
        return file;
    }

    /**
     * Serialize template DTO
     * @param dto template instance
     * @return serialized template as byte array
     * @throws JAXBException
     * @throws XMLStreamException
     * @throws IOException
     */
    public static byte[] serialize(final TemplateDTO dto) throws JAXBException, XMLStreamException, IOException {
        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BufferedOutputStream bos = new BufferedOutputStream(baos)
        ) {
            // using jaxb for serialization because of differences in behaviour between jaxb (used by NiFi) and jackson
            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Marshaller marshaller = context.createMarshaller();
            XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
            marshaller.marshal(dto, xmlof.createXMLStreamWriter(bos));

            bos.flush();
            return baos.toByteArray();
        }
    }

    /**
     * Deserialize template from input stream.
     * @param in input stream of template xml
     * @return template DTO
     * @throws JAXBException
     */
    public static TemplateDTO deserialize(InputStream in) throws JAXBException {
        // using jaxb for deserialization because of differences in behaviour between jaxb (used by NiFi) and jackson
        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        JAXBElement<TemplateDTO> element = unmarshaller.unmarshal(new StreamSource(in), TemplateDTO.class);
        return element.getValue();
    }

    /**
     * Deserialize template from file.
     * @param file template xml file handle
     * @return template DTO
     * @throws IOException
     * @throws JAXBException
     */
    public static TemplateDTO deserialize(File file) throws IOException, JAXBException {
        try (InputStream in = new FileInputStream(file)) {
            return deserialize(in);
        }
    }

}
