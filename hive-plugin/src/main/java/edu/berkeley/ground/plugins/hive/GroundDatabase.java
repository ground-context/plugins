/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroundDatabase {
    private static final String DATABASE = "database";

    static final private Logger logger = LoggerFactory.getLogger(GroundDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final String DB_STATE = "_DATABASE_STATE";

    private static final String DATABASE_PARENT_NODE = "_DATABASE_PARENT_NODE";

    private final GroundReadWrite groundReadWrite;
    private GroundTable groundTable;
    private NodeVersion metaDatabaseNodeVersion = null;

    protected GroundDatabase(GroundReadWrite groundReadWrite) {
        this.groundReadWrite = groundReadWrite;
        // create a parent for all databases - its edges will determine the
        // number of databases
        if (metaDatabaseNodeVersion == null) {
            try {
                Database metaDatabase = new Database(DATABASE_PARENT_NODE, "metanode", "locationurl",
                        new HashMap<String, String>());
                this.metaDatabaseNodeVersion = createDatabaseNodeVersion(metaDatabase, EntityState.ACTIVE.name());
            } catch (InvalidObjectException | MetaException ex) {
                logger.error("error creating metadatabase: {}", ex);
            }
        }
    }

    protected Database getDatabase(String dbName) throws GroundException {
        NodeVersion latestVersion = getDatabaseNodeVersion(dbName);
        logger.debug("database versions id structureVersionId: {}, {}", latestVersion.getNodeId(),
                latestVersion.getStructureVersionId());
        Map<String, Tag> dbTag = latestVersion.getTags();
        return PluginUtil.fromJson((String) dbTag.get(dbName).getValue(), Database.class);
    }

    protected NodeVersion getDatabaseNodeVersion(String dbName) throws GroundException {
        List<Long> versions = (List<Long>) PluginUtil.getLatestVersions(dbName, "nodes");
        if (versions == null || versions.isEmpty()) {
            throw new GroundException("Database node not found: " + dbName);
        }
        logger.debug("database versions size: {}", versions.size());
        return this.groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(versions.get(0));
    }

    protected NodeVersion createDatabaseNodeVersion(Database db, String state)
            throws InvalidObjectException, MetaException {
        if (db == null) {
            throw new InvalidObjectException("Database object passed is null");
        }

        try {
            // create a new tag for new database node version entry
            String dbName = db.getName();
            String reference = dbName;
            Map<String, Tag> tags = new HashMap<>();
            Tag stateTag = new Tag(1L, dbName + state, state, GroundType.STRING);
            tags.put(DB_STATE, stateTag);
            Tag dbTag = new Tag(1L, dbName, PluginUtil.toJson(db), GroundType.STRING);
            tags.put(dbName, dbTag);
            Map<String, String> referenceParameterMap = db.getParameters();
            if (referenceParameterMap == null) {
                referenceParameterMap = new HashMap<String, String>();
            }
            StructureVersion sv = this.getDatabaseStructureVersion(state);
            NodeVersion dbNodeVersion = this.groundReadWrite.getGroundReadWriteNodeResource().createNodeVersion(1L,
                    tags, sv.getId(), reference, referenceParameterMap, dbName);
            if (dbName.equals(DATABASE_PARENT_NODE)) {
                logger.debug("created metanode: {}", dbNodeVersion.getId());
                return dbNodeVersion;
            }
            // create a new edge from just created database to metadatabase node
            // useful for getting all edges
            Edge edge = groundReadWrite.getGroundReadWriteEdgeResource().createEdge(DATABASE_PARENT_NODE + dbName,
                    tags);
            groundReadWrite.getGroundReadWriteEdgeResource().createEdgeVersion(edge.getId(), tags, sv.getId(),
                    reference, referenceParameterMap, edge.getId(), metaDatabaseNodeVersion.getId(),
                    dbNodeVersion.getId());
            return dbNodeVersion;
        } catch (GroundException e) {
            logger.error("Failure to create a database node: {}", e);
            throw new MetaException(e.getMessage());
        }
    }

    private StructureVersion getDatabaseStructureVersion(String state) throws GroundException {
        return this.groundReadWrite.getGroundReadWriteStructureResource().getStructureVersion(DATABASE, state);
    }

    // Table related functions
    protected NodeVersion createTableComponents(Table table) throws InvalidObjectException, MetaException {
        return null;
    }

    protected NodeVersion dropTableNodeVersion(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return null;
    }

    protected Table getTable(String dbName, String tableName) throws MetaException {
        return this.groundTable.getTable(dbName, tableName);
    }

    protected List<String> getTables(String dbName, String pattern) throws MetaException {
        return this.groundTable.getTables(dbName, pattern);
    }

    protected NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        return null;
    }

    protected Partition getPartition(String dbName, String tableName, String partName)
            throws NoSuchObjectException, MetaException {
        try {
            return groundTable.getPartition(dbName, tableName, partName);
        } catch (MetaException | NoSuchObjectException ex) {
            logger.error("Unable to ger partition {} fro table {} database {}", partName, tableName, dbName);
            throw ex;
        }
    }

    protected List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            return groundTable.getPartitions(dbName, tableName, max);
        } catch (MetaException | NoSuchObjectException ex) {
            logger.error("get partitions failed for: {}, {}", dbName, tableName);
            throw ex;
        }
    }

    protected NodeVersion dropDatabase(String dbName, String state) throws GroundException {
        NodeVersion databaseNodeVersion = getDatabaseNodeVersion(dbName);
        Map<String, Tag> dbTagMap = databaseNodeVersion.getTags();
        if (dbTagMap == null) {
            logger.debug("node version getTags failed");
            return null;
        }
        StructureVersion sv = getDatabaseStructureVersion(state);
        Tag stateTag = new Tag(1L, dbName + state, state, GroundType.STRING);
        dbTagMap.put(DB_STATE, stateTag); // update state to deleted
        logger.info("database deleted: {}, {}", dbName, databaseNodeVersion.getNodeId());
        Edge edge = groundReadWrite.getGroundReadWriteEdgeResource().getEdge(DATABASE_PARENT_NODE + dbName);
        // create a edge version for the deleted database using original and
        // newly created "deleted" node version
        NodeVersion deletedDatabaseNodeVersion = this.groundReadWrite.getGroundReadWriteNodeResource()
                .createNodeVersion(1L, dbTagMap, sv.getId(), "", new HashMap<String, String>(), dbName);
        groundReadWrite.getGroundReadWriteEdgeResource().createEdgeVersion(edge.getId(), dbTagMap, sv.getId(), dbName,
                new HashMap<String, String>(), edge.getId(), metaDatabaseNodeVersion.getId(),
                databaseNodeVersion.getId());
        return deletedDatabaseNodeVersion;
    }

    protected List<String> getDatabases(String pattern) throws GroundException {
        List<String> list = new ArrayList<>();
        List<Long> metaDatabaseClosureList = groundReadWrite.getGroundReadWriteNodeResource()
                .getTransitiveClosure(metaDatabaseNodeVersion.getId());
        for (long parent : metaDatabaseClosureList) {
            NodeVersion nodeVersion = this.groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(parent);
            // for each node in closure list get all its children
            List<Long> nodeClosureList = groundReadWrite.getGroundReadWriteNodeResource()
                    .getTransitiveClosure(nodeVersion.getId());
            if (!nodeClosureList.isEmpty()) { // this node has children -
                                              // possibly deleted nodes
                for (long child : nodeClosureList) {
                    Iterator<Tag> tags = this.groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(child)
                            .getTags().values().iterator();
                    while (tags.hasNext()) {
                        logger.info("getDatabases tags: {} {}", tags.next().getKey());
                    }
                }
                logger.info("reference removed is: {}", nodeVersion.getReference());
                list.remove(nodeVersion.getReference());
            } else {
                logger.info("reference added is: {}", nodeVersion.getReference());
                list.add(nodeVersion.getReference());
            }
        }
        return list;
    }
}
