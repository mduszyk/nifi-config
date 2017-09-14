package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.model.ConfigException;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.ControllerServicesApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessorsApi;
import com.github.hermannpencole.nifi.swagger.client.model.ControllerServiceEntity;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessorDTO;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessorEntity;
import com.github.hermannpencole.nifi.swagger.client.model.RelationshipDTO;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * API tests for AccessApi
 */
@RunWith(MockitoJUnitRunner.class)
public class ProcessorServiceTest {

    @Mock
    private ProcessorsApi processorsApiMock;

    @Test
    public void setStateAlreadyTest() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            protected void configure() {
                bind(ProcessorsApi.class).toInstance(processorsApiMock);
                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
            }
        });
        ProcessorService processorService = injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        processor.getComponent().setState(ProcessorDTO.StateEnum.RUNNING);
        processorService.setState(processor, ProcessorDTO.StateEnum.RUNNING);
        verify(processorsApiMock, never()).updateProcessor(anyString(), anyObject());
    }

    @Test
    public void setStateTest() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            protected void configure() {
                bind(ProcessorsApi.class).toInstance(processorsApiMock);
                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
            }
        });
        ProcessorService processorService = injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        processor.getComponent().setState(ProcessorDTO.StateEnum.STOPPED);

        ProcessorEntity processorResponse = TestUtils.createProcessorEntity("id", "name");
        processorResponse.getComponent().setState(ProcessorDTO.StateEnum.RUNNING);
        when(processorsApiMock.updateProcessor(eq("id"), any() )).thenReturn(processorResponse);

        processorService.setState(processor, ProcessorDTO.StateEnum.RUNNING);
        ArgumentCaptor<ProcessorEntity> processorEntity = ArgumentCaptor.forClass(ProcessorEntity.class);
        verify(processorsApiMock).updateProcessor(eq("id"), processorEntity.capture());
        assertEquals("id", processorEntity.getValue().getComponent().getId());
        assertEquals( ProcessorDTO.StateEnum.RUNNING, processorEntity.getValue().getComponent().getState());
    }

    @Test(expected = ConfigException.class)
    public void setStateExceptionTest() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            protected void configure() {
                bind(ProcessorsApi.class).toInstance(processorsApiMock);
                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
            }
        });
        ProcessorService processorService = injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        processor.getComponent().setState(ProcessorDTO.StateEnum.STOPPED);

        when(processorsApiMock.updateProcessor(eq("id"), any() )).thenThrow(new ApiException());

        processorService.setState(processor, ProcessorDTO.StateEnum.RUNNING);
    }

    @Test
    public void updateProcessorTest() {
        // given
        Injector injector = Guice.createInjector(new AbstractModule() {
            protected void configure() {
                bind(ProcessorsApi.class).toInstance(processorsApiMock);
                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
            }
        });
        ProcessorService processorService = injector.getInstance(ProcessorService.class);
        ProcessorEntity processor = TestUtils.createProcessorEntity("id", "name");
        RelationshipDTO inputAutoterminate = new RelationshipDTO().name("success").autoTerminate(true);
        processor.getComponent().addRelationshipsItem(inputAutoterminate);

        // when
        processorService.updateProcessor(processor);

        // then
        ArgumentCaptor<ProcessorEntity> processorEntity = ArgumentCaptor.forClass(ProcessorEntity.class);
        verify(processorsApiMock).updateProcessor(eq("id"), processorEntity.capture());
        assertEquals("id", processorEntity.getValue().getComponent().getId());
        assertEquals(1, processorEntity.getValue().getComponent().getConfig().getAutoTerminatedRelationships().size());
        assertEquals("success", processorEntity.getValue().getComponent().getConfig().getAutoTerminatedRelationships().get(0));
    }

    @Test
    public void updateControllerServiceReferencesTest() {
        // given
        Injector injector = Guice.createInjector(new AbstractModule() {
            protected void configure() {
                bind(ProcessorsApi.class).toInstance(processorsApiMock);
                bind(Integer.class).annotatedWith(Names.named("timeout")).toInstance(1);
                bind(Integer.class).annotatedWith(Names.named("interval")).toInstance(1);
                bind(Boolean.class).annotatedWith(Names.named("forceMode")).toInstance(false);
            }
        });
        ProcessorService processorService = injector.getInstance(ProcessorService.class);

        ProcessorEntity processor1 = TestUtils.createProcessorEntity("p1", "processor1");
        processor1.getComponent().getConfig().getProperties().put("Controller Service 1", "101");
        ProcessorEntity processor2 = TestUtils.createProcessorEntity("p2", "processor2");
        processor2.getComponent().getConfig().getProperties().put("Controller Service 2", "102");
        ProcessorEntity processor3 = TestUtils.createProcessorEntity("p3", "processor3");
        List<ProcessorEntity> processors = Arrays.asList(processor1, processor2, processor3);

        ControllerServiceEntity service1 = TestUtils.createControllerServiceEntity("s1", "service1");
        ControllerServiceEntity service2 = TestUtils.createControllerServiceEntity("s2", "service2");
        ControllerServiceEntity service3 = TestUtils.createControllerServiceEntity("s3", "service3");
        List<ControllerServiceEntity> services = Arrays.asList(service1, service2, service3);

        Map<String, String> serviceIdToName = new HashMap<String, String>() {{
            put("101", "service1");
            put("102", "service2");
        }};

        // when
        processorService.updateControllerServiceReferences(processors, services, serviceIdToName);

        // then
        assertEquals("s1", processor1.getComponent().getConfig().getProperties().get("Controller Service 1"));
        assertEquals("s2", processor2.getComponent().getConfig().getProperties().get("Controller Service 2"));
    }

}