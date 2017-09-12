package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessorsApi;
import com.github.hermannpencole.nifi.swagger.client.TemplatesApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
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


        // this way of deserializing doesn't work
//        String templateStr = Files.toString(file, Charset.forName("UTF-8"));
//        Type returnType = new TypeToken<TemplateDTO>(){}.getType();
//        TemplateDTO t = (new XML()).deserialize(templateStr, returnType);

        // read and deserialize template from file (using nifi-client-dto here)
        org.apache.nifi.web.api.dto.TemplateDTO templ = null;
        try {
            JAXBContext context = JAXBContext.newInstance(org.apache.nifi.web.api.dto.TemplateDTO.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            JAXBElement<org.apache.nifi.web.api.dto.TemplateDTO> templateElement =
                    unmarshaller.unmarshal(
                            new StreamSource(new FileInputStream(file)),
                            org.apache.nifi.web.api.dto.TemplateDTO.class
                    );
            templ = templateElement.getValue();
            System.out.println(templ);
        } catch (Exception e) {
            LOG.error("Failed deserializing template", e);
        }

        // 1. Cache service guid to name mapping from controllerServices section of template.
        Map<String, String> guidToName = templ.getSnippet().getControllerServices().stream()
                .collect(Collectors.toMap(
                        org.apache.nifi.web.api.dto.ControllerServiceDTO::getId,
                        org.apache.nifi.web.api.dto.ControllerServiceDTO::getName
                ));

        // 2. Delete controllerServices from the template before template is uploaded to nifi in order to prevent creation of duplicates.
        File templateTmpFile = null;
        try {
            templ.getSnippet().setControllerServices(new HashSet<>());
            // serialize modified template to temp file
            InputStream templateIn = new ByteArrayInputStream(serialize(templ));
            templateTmpFile = File.createTempFile("nifi", "template");
            OutputStream out = new FileOutputStream(templateTmpFile);
            IOUtils.copy(templateIn, out);
        } catch (Exception e) {
            LOG.error("Failed deserializing template", e);
            throw new ApiException(e);
        }

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
        processorEntities.forEach(processorEntity -> {
            Map<String, String> configProperties = processorEntity.getComponent().getConfig().getProperties();
            configProperties.forEach((key, value) ->
                    Optional.ofNullable(guidToName.get(value))
                            .map(serviceName ->
                                    controllerServiceEntities.stream()
                                            .filter(controllerServiceEntity -> serviceName.equals(controllerServiceEntity.getComponent().getName()))
                                            .findFirst()
                                            .map(controllerServiceEntity -> configProperties.put(key, controllerServiceEntity.getId()))
                            )
            );
        });

        // 6. Update matched controller services references.
        processorEntities.forEach(processorEntity -> processorService.updateProcessor(processorEntity));

    }

    private static byte[] serialize(final org.apache.nifi.web.api.dto.TemplateDTO dto) throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final BufferedOutputStream bos = new BufferedOutputStream(baos);

        JAXBContext context = JAXBContext.newInstance(org.apache.nifi.web.api.dto.TemplateDTO.class);
        Marshaller marshaller = context.createMarshaller();
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        XMLStreamWriter writer = new IndentingXMLStreamWriter(xmlof.createXMLStreamWriter(bos));
        marshaller.marshal(dto, writer);

        bos.flush();
        return baos.toByteArray(); //Note: For really large templates this could use a lot of heap space
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
