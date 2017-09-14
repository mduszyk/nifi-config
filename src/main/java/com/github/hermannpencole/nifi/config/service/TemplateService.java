package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.TemplatesApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.bind.*;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class that offer service for nifi template
 * <p>
 * <p>
 * Created by SFRJ on 01/04/2017.
 */
@Singleton
public class TemplateService {

    /**
     * The logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(TemplateService.class);

    /**
     * The processGroupService nifi.
     */
    @Inject
    private ProcessGroupService processGroupService;

    @Inject
    private ProcessorService processorService;

    @Inject
    private ProcessGroupsApi processGroupsApi;

    @Inject
    private FlowApi flowApi;

    @Inject
    private TemplatesApi templatesApi;

    /**
     * @param branch
     * @param fileConfiguration
     * @throws IOException
     * @throws URISyntaxException
     * @throws ApiException
     */
    public void installOnBranch(List<String> branch, String fileConfiguration) throws ApiException, IOException {
        ProcessGroupFlowDTO processGroupFlow = processGroupService.createDirectory(branch).getProcessGroupFlow();
        File file = new File(fileConfiguration);

        //must we force resintall template ?
        /*TemplatesEntity templates = flowApi.getTemplates();
        String name = FilenameUtils.getBaseName(file.getName());
        Optional<TemplateEntity> template = templates.getTemplates().stream().filter(templateParse -> templateParse.getTemplate().getName().equals(name)).findFirst();
        if (!template.isPresent()) {
            template = Optional.of(processGroupsApi.uploadTemplate(processGroupFlow.getId(), file));
        }*/


        // deserializing template using jaxb and nifi-swagger-client DTOs
        TemplateDTO templ = deserializeTemplate(file);

        // 1. Cache service guid to name mapping from controllerServices section of template.
        Map<String, String> serviceIdToName = templ.getSnippet().getControllerServices().stream()
                .collect(Collectors.toMap(ControllerServiceDTO::getId, ControllerServiceDTO::getName));

        // 2. Delete controllerServices from the template before template is uploaded to nifi in order to prevent creation of duplicates.
        templ.getSnippet().setControllerServices(Collections.emptyList());
        // serialize and store modified template in temp file
        File templateTmpFile = storeTemplate(templ);

        // 3. Pull list of controller services from nifi.
        List<ControllerServiceEntity> controllerServiceEntities = flowApi
                .getControllerServicesFromGroup(processGroupFlow.getId())
                .getControllerServices();


        Optional<TemplateEntity> template = Optional.of(processGroupsApi.uploadTemplate(processGroupFlow.getId(), templateTmpFile));
        InstantiateTemplateRequestEntity instantiateTemplate = new InstantiateTemplateRequestEntity(); // InstantiateTemplateRequestEntity | The instantiate template request.
        instantiateTemplate.setTemplateId(template.get().getTemplate().getId());
        instantiateTemplate.setOriginX(0d);
        instantiateTemplate.setOriginY(0d);
        FlowEntity flow = processGroupsApi.instantiateTemplate(processGroupFlow.getId(), instantiateTemplate);


        // 4. For each processor's property check if its value is present in the cache.
        // 5. If property value is in a cache, using cached name try to match against controller service name in the list pulled in 3.
        List<ProcessorEntity> processorEntities = flow.getFlow().getProcessors();
        updateControllerServiceReferences(processorEntities, controllerServiceEntities, serviceIdToName);

        // 6. Update matched controller services references.
        processorEntities.forEach(processorEntity -> processorService.updateProcessor(processorEntity));
    }

    private void updateControllerServiceReferences(
            List<ProcessorEntity> processorEntities,
            List<ControllerServiceEntity> controllerServiceEntities,
            Map<String, String> serviceIdToName
    ) {
        processorEntities.forEach(processorEntity -> {
            Map<String, String> configProperties = processorEntity.getComponent().getConfig().getProperties();
            configProperties.forEach((key, value) ->
                    Optional.ofNullable(serviceIdToName.get(value))
                            .map(serviceName ->
                                    controllerServiceEntities.stream()
                                            .filter(controllerServiceEntity -> serviceName.equals(controllerServiceEntity.getComponent().getName()))
                                            .findFirst()
                                            .map(controllerServiceEntity -> configProperties.put(key, controllerServiceEntity.getId()))
                            )
            );
        });
    }

    private File storeTemplate(final TemplateDTO template) {
        try {
            File file = File.createTempFile("nifi-template-", ".xml");
            try (
                InputStream in = new ByteArrayInputStream(serializeTemplate(template));
                OutputStream out = new FileOutputStream(file);
            ) {
                IOUtils.copy(in, out);
            }
            return file;
        } catch (Exception e) {
            LOG.error("Failed storing template", e);
            throw new ApiException(e);
        }
    }

    private byte[] serializeTemplate(final TemplateDTO dto) throws JAXBException, XMLStreamException, IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final BufferedOutputStream bos = new BufferedOutputStream(baos);

        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Marshaller marshaller = context.createMarshaller();
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        marshaller.marshal(dto, xmlof.createXMLStreamWriter(bos));

        bos.flush();
        return baos.toByteArray();
    }

    private TemplateDTO deserializeTemplate(InputStream in) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        JAXBElement<TemplateDTO> element = unmarshaller.unmarshal(new StreamSource(in), TemplateDTO.class);
        return element.getValue();
    }

    private TemplateDTO deserializeTemplate(File file) {
        try (InputStream in = new FileInputStream(file)) {
            return deserializeTemplate(in);
        } catch (Exception e) {
            LOG.error("Failed deserializing template", e);
            throw new ApiException(e);
        }
    }

    public void undeploy(List<String> branch) throws ApiException {
        Optional<ProcessGroupFlowEntity> processGroupFlow = processGroupService.changeDirectory(branch);
        if (!processGroupFlow.isPresent()) {
            LOG.warn("cannot find " + Arrays.toString(branch.toArray()));
            return;
        }
        TemplatesEntity templates = flowApi.getTemplates();
        Stream<TemplateEntity> templatesInGroup = templates.getTemplates().stream()
                .filter(templateParse -> templateParse.getTemplate().getGroupId().equals(processGroupFlow.get().getProcessGroupFlow().getId()));
        for (TemplateEntity templateInGroup : templatesInGroup.collect(Collectors.toList())) {
            templatesApi.removeTemplate(templateInGroup.getId());
        }

        //Stop branch
        processGroupService.stop(processGroupFlow.get());
        LOG.info(Arrays.toString(branch.toArray()) + " is stopped");

        //the state change, then the revision also in nifi 1.3.0 (only?) reload processGroup
        ProcessGroupEntity processGroupEntity = processGroupsApi.getProcessGroup(processGroupFlow.get().getProcessGroupFlow().getId());

        processGroupsApi.removeProcessGroup(processGroupFlow.get().getProcessGroupFlow().getId(), processGroupEntity.getRevision().getVersion().toString(),null);

    }


    }
