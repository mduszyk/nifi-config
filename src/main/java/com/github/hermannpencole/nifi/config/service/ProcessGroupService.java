package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class that offer service for process group
 * <p>
 * Created by SFRJ on 01/04/2017.
 */
@Singleton
public class ProcessGroupService {

    /**
     * The logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(ProcessGroupService.class);

    @Inject
    private FlowApi flowapi;

    @Inject
    private ProcessGroupsApi processGroupsApi;

    @Inject
    private ProcessorService processorService;

    @Inject
    private ConnectionService connectionService;

    /**
     * browse nifi on branch pass in parameter
     *
     * @param branch
     * @return
     * @throws ApiException
     */
    public Optional<ProcessGroupFlowEntity> changeDirectory(List<String> branch) throws ApiException {
        ProcessGroupFlowEntity flowEntity = flowapi.getFlow("root");
        for (String processGroupName : branch.subList(1, branch.size())) {
            Optional<ProcessGroupEntity> flowEntityChild = findByComponentName(flowEntity.getProcessGroupFlow().getFlow().getProcessGroups(), processGroupName);
            if (!flowEntityChild.isPresent()) {
                return Optional.empty();
            }
            flowEntity = flowapi.getFlow(flowEntityChild.get().getId());
        }
        return Optional.of(flowEntity);
    }

    //can static => utils
    public static Optional<ProcessGroupEntity> findByComponentName(List<ProcessGroupEntity> listGroup, String name) {
        return listGroup.stream()
                .filter(item -> item.getComponent().getName().trim().equals(name.trim()))
                .findFirst();
    }

    /**
     * browse nifi on branch pass in parameter
     *
     * @param branch
     * @return
     * @throws ApiException
     */
    public ProcessGroupFlowEntity createDirectory(List<String> branch) throws ApiException {
        //generate clientID
        String clientId = flowapi.generateClientId();
        //find root
        ProcessGroupFlowEntity flowEntity = flowapi.getFlow("root");
        for (String processGroupName : branch.subList(1, branch.size())) {
            Optional<ProcessGroupEntity> flowEntityChild = findByComponentName(flowEntity.getProcessGroupFlow().getFlow().getProcessGroups(), processGroupName);
            if (!flowEntityChild.isPresent()) {
                PositionDTO position = getNextPosition(flowEntity);
                ProcessGroupEntity created = new ProcessGroupEntity();
                created.setRevision(new RevisionDTO());
                created.setComponent(new ProcessGroupDTO());
                created.getRevision().setVersion(0L);
                created.getRevision().setClientId(clientId);
                created.getComponent().setName(processGroupName);
                created.getComponent().setPosition(position);
                created = processGroupsApi.createProcessGroup(flowEntity.getProcessGroupFlow().getId(), created);
                flowEntity = flowapi.getFlow(created.getId());
            } else {
                flowEntity = flowapi.getFlow(flowEntityChild.get().getId());
            }
        }
        return flowEntity;
    }


    /**
     * set state on entire process group (no report error if there is)
     *
     * @param id
     * @param state
     * @throws ApiException
     */
    public void setState(String id, ScheduleComponentsEntity.StateEnum state) throws ApiException {
        ScheduleComponentsEntity body = new ScheduleComponentsEntity();
        body.setId(id);
        body.setState(state);
        body.setComponents(null);//for all
        flowapi.scheduleComponents(id, body);
    }

    /**
     * start the processor group.
     * Begin by processor that consumme flow and end with processor that consumme stream and create flow
     *
     * @param processGroupFlow
     * @throws ApiException
     */
    public void start(ProcessGroupFlowEntity processGroupFlow) throws ApiException {
        try {
            FlowDTO flow = processGroupFlow.getProcessGroupFlow().getFlow();
            List<Set<?>> listing = reorder(flow);
            for (int i = (listing.size() - 1); i >= 0; i--) {
                Set<?> set = listing.get(i);
                for (Object object : set) {
                    if (object instanceof ProcessorEntity) {
                        processorService.setState((ProcessorEntity) object, ProcessorDTO.StateEnum.RUNNING);
                    }
                }
            }
            for (ProcessGroupEntity procGroupInConf : flow.getProcessGroups()) {
                ProcessGroupFlowEntity processGroupFlowEntity = flowapi.getFlow(procGroupInConf.getId());
                start(processGroupFlowEntity);
            }
        } catch (Exception e) {
            setState(processGroupFlow.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.STOPPED);
            throw e;
        }
    }

    /**
     * stop the processor group.
     * Begin by processor that consumme stream and create flow and end with processor that consumme flow.
     *
     * @param processGroupFlow
     * @throws ApiException
     */
    public void stop(ProcessGroupFlowEntity processGroupFlow) throws ApiException {
        try {
            FlowDTO flow = processGroupFlow.getProcessGroupFlow().getFlow();
            List<Set<?>> listing = reorder(flow);
            for (int i = 0; i < (listing.size()); i++) {
                Set<?> set = listing.get(i);
                for (Object object : set) {
                    if (object instanceof ProcessorEntity) {
                        processorService.setState((ProcessorEntity) object, ProcessorDTO.StateEnum.STOPPED);
                    } else if (object instanceof ConnectionEntity) {
                        connectionService.waitEmptyQueue((ConnectionEntity) object);
                    }
                }
            }
            for (ProcessGroupEntity procGroupInConf : flow.getProcessGroups()) {
                ProcessGroupFlowEntity processGroupFlowEntity = flowapi.getFlow(procGroupInConf.getId());
                start(processGroupFlowEntity);
            }
        } catch (Exception e) {
            setState(processGroupFlow.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.RUNNING);
            throw e;
        }
    }

    /**
     * reorder for have the processor that consume stream -> connection -> processor connected etcc ...in the good order.
     *
     * @param flow
     * @return
     */
    public List<Set<?>> reorder(FlowDTO flow) {
        List<Set<?>> level = new ArrayList<>();
        Set<String> thisLevel = new HashSet<>();
        List<ConnectionEntity> connections = new ArrayList<>(flow.getConnections());
        while (!connections.isEmpty()) {
            Set<String> destination = new HashSet<>();
            Set<String> source = new HashSet<>();
            Set<ConnectionEntity> levelConnection= new HashSet<>();
            for (ConnectionEntity connection: new ArrayList<>(connections)) {
                if (thisLevel.contains(connection.getSourceId())) {
                    source.add(connection.getDestinationId());
                    //remove connection for next use
                    connections.remove(connection);
                    levelConnection.add(connection);
                } else {
                    destination.add(connection.getDestinationId());
                    source.add(connection.getSourceId());
                }
            }
            if (!levelConnection.isEmpty()) {
                level.add(levelConnection);
            }
            thisLevel = new HashSet<>(source);
            thisLevel.removeAll(destination);
            Set<ProcessorEntity> levelProcessor= new HashSet<>();
            for (ProcessorEntity processor : flow.getProcessors()){
                if (thisLevel.contains(processor.getId())) {
                    levelProcessor.add(processor);
                }
            }
            level.add(levelProcessor);
        }
        return level;
    }

    /**
     * get the next free position to place the processor(or group processor) on this group processor
     *
     * @param flowEntity
     * @return
     */
    public PositionDTO getNextPosition(ProcessGroupFlowEntity flowEntity) {
        PositionDTO nextPosition = new PositionDTO();
        List<PositionDTO> positions = new ArrayList<>();

        for (ProcessorEntity processor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            positions.add(processor.getPosition());
        }
        for (ProcessGroupEntity processGroup : flowEntity.getProcessGroupFlow().getFlow().getProcessGroups()) {
            positions.add(processGroup.getPosition());
        }
        for (PortEntity port : flowEntity.getProcessGroupFlow().getFlow().getInputPorts()) {
            positions.add(port.getPosition());
        }
        for (PortEntity port : flowEntity.getProcessGroupFlow().getFlow().getOutputPorts()) {
            positions.add(port.getPosition());
        }
        for (ConnectionEntity conn : flowEntity.getProcessGroupFlow().getFlow().getConnections()) {
            positions.add(conn.getPosition());
        }
        for (FunnelEntity funnel : flowEntity.getProcessGroupFlow().getFlow().getFunnels()) {
            positions.add(funnel.getPosition());
        }
        for (LabelEntity label : flowEntity.getProcessGroupFlow().getFlow().getLabels()) {
            positions.add(label.getPosition());
        }

        // remove nulls from positions list
        positions = positions.stream().filter(Objects::nonNull).collect(Collectors.toList());

        double x = 0, y = 0;
        while(containsPosition(positions, x, y)) {
            if (x + ELEMENT_WIDTH > MAX_X) {
                x = 0;
                y += ELEMENT_HEIGHT;
            } else {
                x += ELEMENT_WIDTH;
            }
        }

        nextPosition.setX(x);
        nextPosition.setY(y);

        LOG.debug("nest postion {},{}", nextPosition.getX(), nextPosition.getY());
        return nextPosition;
    }

    // this has to be pre-configured because there is no way to retrieve element size from NiFi
    private final static double ELEMENT_WIDTH = 500;
    private final static double ELEMENT_HEIGHT = 350;
    private final static double MAX_X = 5000;

    private boolean containsPosition(List<PositionDTO> positions, double x, double y) {
        return positions.stream().anyMatch(positionDTO -> {
            double posX = positionDTO.getX();
            double posY = positionDTO.getY();
            return ((posX <= x && x <= posX + ELEMENT_WIDTH) || (posX <= x + ELEMENT_WIDTH && x <= posX))
                    && ((posY <= y && y <= posY + ELEMENT_HEIGHT) || (posY <= y + ELEMENT_HEIGHT && y <= posY));
        });
    }

}
