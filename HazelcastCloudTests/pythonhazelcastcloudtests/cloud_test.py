import os
import random
import time
import unittest
from os.path import abspath

import hazelcast
import logging

from parameterized import parameterized
from hazelcast.discovery import HazelcastCloudDiscovery
from hazelcast.errors import IllegalStateError
from hzrc.client import HzRemoteController
from hzrc.ttypes import CloudCluster

HZ_VERSION = os.getenv("HZ_VERSION")

HazelcastCloudDiscovery._CLOUD_URL_BASE = os.getenv("BASE_URL").replace("https://", "")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test")

class StandardClusterTests(unittest.TestCase):
    cluster: CloudCluster = None
    rc: HzRemoteController = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.rc = HzRemoteController("127.0.0.1", 9701)
        cls.rc.loginToCloudUsingEnvironment()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.rc.exit()

    def tearDown(self) -> None:
        self.rc.deleteCloudCluster(self.cluster.id)

    @parameterized.expand([(True, True), (False, True), (True, False), (False, False)])
    def test_cloud(self, smart_routing, tls_enabled):
        if tls_enabled:
            logger.error(
                f"Create TLS enabled client config for smart routing : {smart_routing}"
            )
            self.cluster = self.rc.createCloudCluster(HZ_VERSION, True)
            client = hazelcast.HazelcastClient(
                **create_client_config_with_ssl(
                    self.cluster.releaseName,
                    self.cluster.token,
                    smart_routing,
                    self.cluster.certificatePath,
                    self.cluster.tlsPassword,
                )
            )
        else:
            logger.error(
                f"Create TLS disabled client config for smart routing {smart_routing}"
            )
            self.cluster = self.rc.createCloudCluster(HZ_VERSION, False)
            client = hazelcast.HazelcastClient(
                **create_client_config(
                    self.cluster.releaseName, self.cluster.token, smart_routing
                )
            )

        map_put_get_and_verify(self, client.get_map("map_for_test_cloud").blocking())

        logger.error("Stopping cluster")
        self.cluster = self.rc.stopCloudCluster(self.cluster.id)
        log_cloud_cluster(self.cluster)

        logger.error("Resuming cluster")
        self.cluster = self.rc.resumeCloudCluster(self.cluster.id)
        log_cloud_cluster(self.cluster)

        logger.error("Wait 5 seconds to be sure client is connected")
        time.sleep(5)
        map_put_get_and_verify(self, client.get_map("map_for_test_cloud").blocking())

        client.shutdown()

    @parameterized.expand([(True,), (False,)])
    def test_try_connect_ssl_cluster_without_certificates(self, smart_routing):
        self.cluster = self.rc.createCloudCluster(HZ_VERSION, True)
        with self.assertRaises(IllegalStateError):
            config = create_client_config(
                self.cluster.releaseName, self.cluster.token, smart_routing
            )
            config["ssl_enabled"] = True
            config["cluster_connect_timeout"] = 10
            hazelcast.HazelcastClient(**config)


def create_client_config(cluster_name, discovery_token, smart_routing):
    config = {
        "cluster_name": cluster_name,
        "cloud_discovery_token": discovery_token,
        "statistics_enabled": True,
        "smart_routing": smart_routing,
    }
    return config


def create_client_config_with_ssl(
    cluster_name, discovery_token, smart_routing, certificates_path, tls_password
):
    config = create_client_config(cluster_name, discovery_token, smart_routing)
    config["ssl_cafile"] = abspath(os.path.join(certificates_path, "ca.pem"))
    config["ssl_certfile"] = abspath(os.path.join(certificates_path, "cert.pem"))
    config["ssl_keyfile"] = abspath(os.path.join(certificates_path, "key.pem"))
    config["ssl_password"] = tls_password
    config["ssl_enabled"] = True
    return config


def map_put_get_and_verify(test_instance, test_map):
    print("Put get to map and verify")
    test_map.clear()
    while test_map.size() < 20:
        random_key = random.randint(1, 100000)
        try:
            test_map.put("key" + str(random_key), "value" + str(random_key))
        except:
            logging.exception("Put operation failed!")

    test_instance.assertEqual(test_map.size(), 20, "Map size should be 20")


def log_cloud_cluster(cluster: CloudCluster):
    logger.error(f"Id: {cluster.id}")
    logger.error(f"Name: {cluster.name}")
    logger.error(f"Release name: {cluster.releaseName}")
    logger.error(f"Hazelcast version: {cluster.hazelcastVersion}")
    logger.error(f"State: {cluster.state}")
    logger.error(f"Discovery token: {cluster.token}")
    logger.error(f"Certificate path: {cluster.certificatePath}")
    logger.error(f"TLS enabled: {cluster.isTlsEnabled}")
    logger.error(f"TLS password: {cluster.tlsPassword}")
