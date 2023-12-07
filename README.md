# Neo4j shortest path without 2hop index

## How to run

Sure, here is the translation:

1. Pull the latest Neo4j source code from https://github.com/neo4j/neo4j.
2. Download the binary executable file for JDK 17 from https://www.oracle.com/java/technologies/downloads/#java17.
3. Download the binary executable file for the latest version of Maven from https://maven.apache.org/download.cgi.
4. Configure environment variables for JDK 17 and Maven:

    ```bash
    export JAVA_HOME=/path/to/jdk17
    export PATH=$JAVA_HOME/bin:$PATH
    export MAVEN_HOME=/path/to/maven
    export PATH=$MAVEN_HOME/bin:$PATH
    ```

5. In the root directory of the Neo4j source code, execute `mvn clean install -T1C -DskipTests` to compile the Neo4j source code. (We will skip the tests, delete the `-DskipTests` parameter to run the tests.)
6. In the `packaging/standalone/target` directory under the Neo4j source code root, extract `neo4j-community-5.13.0-SNAPSHOT-unix.tar.gz` to obtain the executable files for Neo4j

    Set environment variables similarly:

    ```bash
    export NEO4J_HOME=/path/to/neo4j
    export PATH=$NEO4J_HOME/bin:$PATH
    ```

7. Test the connection and modify the initial password:

    ```bash
    neo4j-admin server start
    cypher-shell
    # Follow the prompts to change the initial password; the initial account and password are both neo4j
    neo4j-admin server stop
    ```

8. Install Neo4j Graph Data Science to support graph data science algorithms.

    Refer to https://neo4j.com/docs/graph-data-science/current/installation/.

9. Install the Python driver:

    ```bash
    pip install neo4j
    ```

10. Run:

    ```bash
    neo4j-admin server start
    # Wait for around 10 seconds, allowing Neo4j to start
    python main.py # Enter the Neo4j password; the password won't be displayed, similar to most password entry methods on Linux
    neo4j-admin server stop
    ```

11. View the results in `results/neo4j_exp_case_*.csv` and terminal output.