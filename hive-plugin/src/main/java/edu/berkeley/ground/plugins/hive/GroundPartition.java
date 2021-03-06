/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package edu.berkeley.ground.plugins.hive;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

public class GroundPartition {

  static final private Logger logger = LoggerFactory.getLogger(GroundTable.class.getName());

  private static final long DUMMY_NOT_USED = 1L;
  private final GroundReadWrite groundReadWrite;

  public GroundPartition(GroundReadWrite ground) {
    groundReadWrite = ground;
  }

  public Node getNode(String partitionName) throws GroundException {
    logger.debug("Fetching partition node: " + partitionName);
    return groundReadWrite.getGroundReadWriteNodeResource().getNode(partitionName);
  }

  public Structure getNodeStructure(String partitionName) throws GroundException {
    try {
      Node node = this.getNode(partitionName);
      return this.groundReadWrite.getGroundReadWriteStructureResource()
          .getStructure(node.getName());
    } catch (GroundException e) {
      logger.error("Unable to fetch parition node structure");
      throw new GroundException(e);
    }
  }

  public Edge getEdge(String partitionName) throws GroundException {
    logger.debug("Fetching table partition edge: " + partitionName);
    return groundReadWrite.getGroundReadWriteEdgeResource().getEdge(partitionName);
  }

  public Structure getEdgeStructure(String partitionName) throws GroundException {
    Edge edge = getEdge(partitionName);
    return groundReadWrite.getGroundReadWriteStructureResource().getStructure(edge.getName());
  }

  public NodeVersion createPartition(String dbName, String tableName, Partition part)
      throws InvalidObjectException, MetaException {
    try {
      ObjectPair<String, String> objectPair =
          new ObjectPair<>(HiveStringUtils.normalizeIdentifier(dbName),
              HiveStringUtils.normalizeIdentifier(tableName));
      String partId = objectPair.toString();
      for (String value : part.getValues()) {
        partId += ":" + value;
      }

      Tag partTag = new Tag(DUMMY_NOT_USED, partId, PluginUtil.toJson(part), GroundType.STRING);
      Map<String, GroundType> structureVersionAttribs = new HashMap<>();
      structureVersionAttribs.put(GroundStore.EntityState.ACTIVE.name(), GroundType.STRING);
      StructureVersion sv = groundReadWrite.getGroundReadWriteStructureResource()
          .getStructureVersion(partId, structureVersionAttribs);
      String reference = part.getSd().getLocation();
      HashMap<String, Tag> tags = new HashMap<>();
      tags.put(partId, partTag);

      long versionId = sv.getId();
      Map<String, String> parameters = part.getParameters();
      return groundReadWrite.getGroundReadWriteNodeResource().createNodeVersion(1L, tags, versionId,
          reference, parameters, partId);
    } catch (GroundException e) {
      throw new MetaException("Unable to create partition " + e.getMessage());
    }
  }
}
