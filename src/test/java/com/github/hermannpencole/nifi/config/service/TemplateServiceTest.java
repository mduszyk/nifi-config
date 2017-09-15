package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.utils.TemplateUtils;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.TemplatesApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Template;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * API tests for AccessApi
 */
@RunWith(MockitoJUnitRunner.class)
public class TemplateServiceTest {
    @Mock
    private ProcessGroupService processGroupServiceMock;
    @Mock
    private ProcessGroupsApi processGroupsApiMock;
    @Mock
    private TemplatesApi templatesApiMock;
    @Mock
    private FlowApi flowApiMock;
    @Mock
    private ProcessorService processorServiceMock;
    @InjectMocks
    private TemplateService templateService;

    /**
     * Creates a token for accessing the REST API via username/password
     * <p>
     * The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header in the format &#39;Authorization: Bearer &lt;token&gt;&#39;.
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void createAccessTokenTest() throws ApiException, IOException, URISyntaxException {
        List<String> branch = Arrays.asList("root", "elt1");
        String fileName = "test";
        ProcessGroupFlowEntity response = TestUtils.createProcessGroupFlowEntity("idProcessGroupFlow", "nameProcessGroupFlow");
        when(processGroupServiceMock.createDirectory(branch)).thenReturn(response);
        TemplateEntity template = new TemplateEntity();
        template.setId("idTemplate");
        template.setTemplate(new TemplateDTO());
        template.getTemplate().setGroupId("idProcessGroupFlow");
        template.getTemplate().setId("idTemplate");
        when(processGroupsApiMock.uploadTemplate(anyString(), any())).thenReturn(template);
        //when(processGroupsApiMock.uploadTemplate(processGroupFlow.getId(), new File(fileName))).thenReturn(template);

        templateService.installOnBranch(branch, fileName);

        InstantiateTemplateRequestEntity instantiateTemplate = new InstantiateTemplateRequestEntity(); // InstantiateTemplateRequestEntity | The instantiate template request.
        instantiateTemplate.setTemplateId(template.getId());
        instantiateTemplate.setOriginX(0d);
        instantiateTemplate.setOriginY(0d);

        verify(processGroupServiceMock).createDirectory(branch);
        verify(processGroupsApiMock).uploadTemplate(response.getProcessGroupFlow().getId(), new File(fileName));
        verify(processGroupsApiMock).instantiateTemplate(response.getProcessGroupFlow().getId(), instantiateTemplate);
    }


    @Test
    public void undeployTest() throws ApiException {
        List<String> branch = Arrays.asList("root", "elt1");
        ProcessGroupFlowEntity response = TestUtils.createProcessGroupFlowEntity("idProcessGroupFlow", "nameProcessGroupFlow");
        Optional<ProcessGroupFlowEntity> processGroupFlow = Optional.of(response);

        when(processGroupServiceMock.changeDirectory(branch)).thenReturn(processGroupFlow);

        TemplatesEntity templates = new TemplatesEntity();
        TemplateEntity template = new TemplateEntity();
        template.setId("templateId");
        template.setTemplate(new TemplateDTO());
        template.getTemplate().setGroupId(processGroupFlow.get().getProcessGroupFlow().getId());
        template.getTemplate().setId("templateId");
        templates.addTemplatesItem(template);
        when(flowApiMock.getTemplates()).thenReturn(templates);

        ProcessGroupEntity processGroupEntity = TestUtils.createProcessGroupEntity("idProcessGroupFlow", "nameProcessGroupFlow");
        when(processGroupsApiMock.getProcessGroup(processGroupFlow.get().getProcessGroupFlow().getId())).thenReturn(processGroupEntity);

        templateService.undeploy(branch);
        verify(templatesApiMock).removeTemplate(template.getId());
        verify(processGroupServiceMock).stop(processGroupFlow.get());
        verify(processGroupsApiMock).removeProcessGroup(processGroupFlow.get().getProcessGroupFlow().getId(), "10", null);
    }

    @Test
    public void undeployNoExistTest() throws ApiException {
        List<String> branch = Arrays.asList("root", "elt1");
        Optional<ProcessGroupFlowEntity> processGroupFlow = Optional.empty();
        when(processGroupServiceMock.changeDirectory(branch)).thenReturn(processGroupFlow);
        templateService.undeploy(branch);
        verify(flowApiMock, never()).getTemplates();
    }

    @Test
    public void installOnBranchUseParentServiceTest() throws IOException, JAXBException {
        // given
        String serviceIdNiFi = "1234";
        String serviceNameProperties = "Database Connection Pooling Service";
        List<String> branch = Arrays.asList("root", "reusable_templates", "service-dependency");
        String templatePath = "src/test/resources/template-service-dependency.xml";
        String processGroupFlowId = "asdf";

        ProcessGroupFlowEntity processGroupFlowEntity = new ProcessGroupFlowEntity();
        ProcessGroupFlowDTO processGroupFlowDTO = new ProcessGroupFlowDTO();
        processGroupFlowDTO.setId(processGroupFlowId);
        processGroupFlowEntity.setProcessGroupFlow(processGroupFlowDTO);
        when(processGroupServiceMock.createDirectory(any())).thenReturn(processGroupFlowEntity);

        ControllerServiceEntity controllerServiceEntity = new ControllerServiceEntity();
        ControllerServiceDTO controllerServiceDTO = new ControllerServiceDTO();
        controllerServiceDTO.setId(serviceIdNiFi);
        controllerServiceDTO.setName("ThriftConnectionPool");
        controllerServiceEntity.setComponent(controllerServiceDTO);
        controllerServiceEntity.setId(serviceIdNiFi);
        List<ControllerServiceEntity> services = Arrays.asList(controllerServiceEntity);
        ControllerServicesEntity servicesEntity = new ControllerServicesEntity();
        servicesEntity.setControllerServices(services);
        when(flowApiMock.getControllerServicesFromGroup(any())).thenReturn(servicesEntity);

        TemplateEntity templateEntity = new TemplateEntity();
        TemplateDTO templateDTO = TemplateUtils.deserialize(new File(templatePath));
        templateEntity.setTemplate(templateDTO);
        when(processGroupServiceMock.uploadTemplate(any(), any())).thenReturn(templateEntity);

        ProcessorEntity processor1 = TestUtils.createProcessorEntity("id", "name");
        processor1.getComponent().getConfig().getProperties()
                .put(serviceNameProperties, "6199301a-015e-1000-0000-000000000000");

        List<ProcessorEntity> processors = Arrays.asList(processor1);
        FlowEntity flowEntity = new FlowEntity();
        FlowDTO flowDTO = new FlowDTO();
        flowDTO.setProcessors(processors);
        flowEntity.setFlow(flowDTO);
        when(processGroupsApiMock.instantiateTemplate(any(), any())).thenReturn(flowEntity);

        doCallRealMethod().when(processorServiceMock).updateControllerServiceReferences(any(), any(), any());


        // when
        templateService.installOnBranchUseParentService(branch, templatePath);


        // then
        verify(processGroupServiceMock).createDirectory(branch);
        verify(flowApiMock).getControllerServicesFromGroup(processGroupFlowId);

        ArgumentCaptor<TemplateDTO> templateCaptor = ArgumentCaptor.forClass(TemplateDTO.class);
        verify(processGroupServiceMock).uploadTemplate(eq(processGroupFlowId), templateCaptor.capture());
        templateDTO.getSnippet().setControllerServices(Collections.emptyList());
        assertEquals(templateDTO, templateCaptor.getValue());

        ArgumentCaptor<InstantiateTemplateRequestEntity> instantiateCaptor = ArgumentCaptor.forClass(InstantiateTemplateRequestEntity.class);
        verify(processGroupsApiMock).instantiateTemplate(eq(processGroupFlowId), instantiateCaptor.capture());
        assertEquals(templateDTO.getId(), instantiateCaptor.getValue().getTemplateId());

        ArgumentCaptor<ProcessorEntity> processorCaptor = ArgumentCaptor.forClass(ProcessorEntity.class);
        verify(processorServiceMock).updateProcessor(processorCaptor.capture());
        assertEquals(serviceIdNiFi, processorCaptor.getValue().getComponent().getConfig().getProperties().get(serviceNameProperties));
    }

}