package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.utils.TemplateUtils;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.TemplatesApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
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
    public void installOnBranch(List<String> branch, String fileConfiguration) throws ApiException {
        ProcessGroupFlowDTO processGroupFlow = processGroupService.createDirectory(branch).getProcessGroupFlow();
        File file = new File(fileConfiguration);

        //must we force resintall template ?
        /*TemplatesEntity templates = flowApi.getTemplates();
        String name = FilenameUtils.getBaseName(file.getName());
        Optional<TemplateEntity> template = templates.getTemplates().stream().filter(templateParse -> templateParse.getTemplate().getName().equals(name)).findFirst();
        if (!template.isPresent()) {
            template = Optional.of(processGroupsApi.uploadTemplate(processGroupFlow.getId(), file));
        }*/
        Optional<TemplateEntity> template = Optional.of(processGroupsApi.uploadTemplate(processGroupFlow.getId(), file));
        InstantiateTemplateRequestEntity instantiateTemplate = new InstantiateTemplateRequestEntity(); // InstantiateTemplateRequestEntity | The instantiate template request.
        instantiateTemplate.setTemplateId(template.get().getTemplate().getId());
        instantiateTemplate.setOriginX(0d);
        instantiateTemplate.setOriginY(0d);
        processGroupsApi.instantiateTemplate(processGroupFlow.getId(), instantiateTemplate);
    }

    /**
     * Install template on branch and try to use controller services from parent if they are present.
     *
     * @param branch
     * @param fileConfiguration
     * @throws ApiException
     */
    public void installOnBranchUseParentService(List<String> branch, String fileConfiguration) throws ApiException {
        try {
            ProcessGroupFlowDTO processGroupFlow = processGroupService.createDirectory(branch).getProcessGroupFlow();
            File file = new File(fileConfiguration);

            TemplateDTO template = TemplateUtils.deserialize(file);

            // Cache service id to name mapping from controllerServices section of template.
            Map<String, String> serviceIdToName = template.getSnippet().getControllerServices().stream()
                    .collect(Collectors.toMap(ControllerServiceDTO::getId, ControllerServiceDTO::getName));

            // Pull list of controller services from NiFi.
            List<ControllerServiceEntity> controllerServiceEntities = flowApi
                    .getControllerServicesFromGroup(processGroupFlow.getId())
                    .getControllerServices();

            // Delete from the template controller services already present in NiFi.
            List<ControllerServiceDTO> filteredServices = template.getSnippet().getControllerServices().stream()
                    .filter(service -> controllerServiceEntities.stream()
                            .noneMatch(serviceNiFi -> service.getName().equals(serviceNiFi.getComponent().getName())))
                    .collect(Collectors.toList());
            template.getSnippet().setControllerServices(filteredServices);

            Optional<TemplateEntity> templateEntity = Optional.of(processGroupService.uploadTemplate(processGroupFlow.getId(), template));
            InstantiateTemplateRequestEntity instantiateTemplate = new InstantiateTemplateRequestEntity();
            instantiateTemplate.setTemplateId(templateEntity.get().getTemplate().getId());
            instantiateTemplate.setOriginX(0d);
            instantiateTemplate.setOriginY(0d);
            FlowEntity flow = processGroupsApi.instantiateTemplate(processGroupFlow.getId(), instantiateTemplate);

            List<ProcessorEntity> processorEntities = flow.getFlow().getProcessors();
            processorService.updateControllerServiceReferences(processorEntities, controllerServiceEntities, serviceIdToName);

            // Update matched controller services references.
            processorEntities.forEach(processorEntity -> processorService.updateProcessor(processorEntity));
        } catch (Exception e) {
            LOG.error("Failed storing template", e);
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
