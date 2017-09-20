package com.github.hermannpencole.nifi.utils;

import com.github.hermannpencole.nifi.config.utils.TemplateUtils;
import com.github.hermannpencole.nifi.swagger.client.model.TemplateDTO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TemplateUtilsTest {

    private static final String TEMPLATE_PATH = "src/test/resources/template-service-dependency.xml";

    @Test
    public void deserializeTest() throws IOException, JAXBException {
        // given
        File file = new File(TEMPLATE_PATH);

        // when
        TemplateDTO template = TemplateUtils.deserialize(file);

        // then
        assertEquals("service-dependency", template.getName());
        assertEquals("7055428e-015e-1000-4e65-f1db18b4cc4e", template.getGroupId());
        assertEquals(1, template.getSnippet().getControllerServices().size());
        assertEquals(1, template.getSnippet().getProcessors().size());
    }

    @Test
    public void serializeDeserializeTest() throws IOException, JAXBException, XMLStreamException {
        // given
        TemplateDTO templateOrig = TemplateUtils.deserialize(new File(TEMPLATE_PATH));

        // when
        byte[] templateSerialized = TemplateUtils.serialize(templateOrig);
        try (ByteArrayInputStream in = new ByteArrayInputStream(templateSerialized)) {
            TemplateDTO templateDeserialized = TemplateUtils.deserialize(in);

            // then
            assertEquals(templateOrig, templateDeserialized);
        }
    }

    @Test
    public void storeTmpFileTest() throws IOException, JAXBException, XMLStreamException {
        // given
        TemplateDTO templateOrig = TemplateUtils.deserialize(new File(TEMPLATE_PATH));

        // when
        File templateStored = TemplateUtils.serializeAndStoreTmpFile(templateOrig);
        try {
            TemplateDTO templateDeserialized = TemplateUtils.deserialize(templateStored);

            // then
            assertEquals(templateOrig, templateDeserialized);
        } finally {
            templateStored.delete();
        }
    }

}

