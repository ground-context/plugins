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
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.stream.JsonReader;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

public class GroundReadWriteEdgeResource {

  public Edge getEdge(String edgeName) throws GroundException {
    GetMethod get = new GetMethod(PluginUtil.buildURL("edges", edgeName));
    return getEdge(get);
  }

  public EdgeVersion getEdgeVersion(long edgeId) throws GroundException {
    GetMethod get = new GetMethod(PluginUtil.groundServerAddress + "edges/versions" + edgeId);
    try {
      return getEdgeVersion(get);
    } catch (IOException e) {
      throw new GroundException(e);
    }
  }

  // create edge for input Tag
  public Edge createEdge(String name, Map<String, Tag> tagMap) throws GroundException {

    try {
      // String encodedUri = PluginUtil.groundServerAddress + "edges/" + URLEncoder.encode(name,
      // "UTF-8");
      String encodedUri = PluginUtil.buildURL("edges", name);
      PostMethod post = new PostMethod(encodedUri);
      ObjectMapper objectMapper = new ObjectMapper();
      String jsonString = objectMapper.writeValueAsString(tagMap);
      post.setRequestEntity(PluginUtil.createRequestEntity(jsonString));
      String response = PluginUtil.execute(post);
      return this.constructEdge(response);
    } catch (IOException e) {
      throw new GroundException(e);
    }
  }

  // method to create the edgeVersion given the nodeId and the tags
  public EdgeVersion createEdgeVersion(long id, Map<String, Tag> tags, long structureVersionId,
      String reference, Map<String, String> referenceParameters, long edgeId, long fromId,
      long toId) throws GroundException {
    try {
      EdgeVersion edgeVersion = new EdgeVersion(id, tags, structureVersionId, reference,
          referenceParameters, edgeId, fromId, toId);
      ObjectMapper mapper = new ObjectMapper();
      String jsonRecord = mapper.writeValueAsString(edgeVersion);
      String uri = PluginUtil.groundServerAddress + "edges/versions";
      PostMethod post = new PostMethod(uri);
      StringRequestEntity requestEntity = PluginUtil.createRequestEntity(jsonRecord);
      post.setRequestEntity(requestEntity);
      return getEdgeVersion(post);
    } catch (IOException e) {
      throw new GroundException(e);
    }
  }

  private Edge getEdge(HttpMethod method) throws GroundException {
    try {
      if (PluginUtil.client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
        // getting the nodeId of the node created
        String response = method.getResponseBodyAsString();
        return constructEdge(response);
      }
      return null;
    } catch (IOException e) {
      throw new GroundException(e);
    }
  }

  private EdgeVersion getEdgeVersion(HttpMethod method)
      throws JsonParseException, JsonMappingException, HttpException, IOException {
    if (PluginUtil.client.executeMethod(method) == HttpURLConnection.HTTP_OK) {
      // create edge version from response of POST request
      String response = method.getResponseBodyAsString();
      JsonReader reader = new JsonReader(new StringReader(response));
      return PluginUtil.fromJson(reader, EdgeVersion.class);
    }
    return null;
  }

  private Edge constructEdge(String response) throws GroundException {
    JsonReader reader = new JsonReader(new StringReader(response));
    return PluginUtil.fromJson(reader, Edge.class);
  }


}
