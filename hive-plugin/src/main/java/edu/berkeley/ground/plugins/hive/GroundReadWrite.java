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

import java.io.IOException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

public class GroundReadWrite {

  public static final String NO_CACHE_CONF = "no_cache_conf";
  private final GroundReadWriteNodeResource groundReadWriteNodeResource =
      new GroundReadWriteNodeResource();
  private final GroundReadWriteStructureResource groundReadWriteStructureResource =
      new GroundReadWriteStructureResource();
  private final GroundReadWriteEdgeResource groundReadWriteEdgeResource =
      new GroundReadWriteEdgeResource();

  String checkStatus(HttpMethod method) throws GroundException {
    try {
      if (PluginUtil.client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
        ObjectMapper objectMapper = new ObjectMapper();
        String text = method.getResponseBodyAsString();
        JsonNode jsonNode = objectMapper.readValue(text, JsonNode.class);
        JsonNode nodeId = jsonNode.get("id");
        return nodeId.asText();
      }
    } catch (IOException e) {
      throw new GroundException(e);
    }
    return null;
  }

  public GroundReadWriteNodeResource getGroundReadWriteNodeResource() {
    return this.groundReadWriteNodeResource;
  }

  public GroundReadWriteStructureResource getGroundReadWriteStructureResource() {
    return this.groundReadWriteStructureResource;
  }

  public GroundReadWriteEdgeResource getGroundReadWriteEdgeResource() {
    return this.groundReadWriteEdgeResource;
  }
}
