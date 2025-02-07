from dataclasses import dataclass
from paramiko import SSHClient
from scp import SCPClient
from husfort.qutility import SFY, SFG


@dataclass
class CHost:
    hostname: str
    username: str
    port: int


def scp_from_remote(host: CHost, remote_path: str, local_path: str, recursive: bool = False):
    with SSHClient() as ssh:
        ssh.load_system_host_keys()
        ssh.connect(hostname=host.hostname, username=host.username, port=host.port)
        with SCPClient(ssh.get_transport()) as scp:
            scp.get(remote_path=remote_path, local_path=local_path, recursive=recursive)
            print(f"[INF] copy {SFY(remote_path)} to {SFG(local_path)}")
    return 0


def scp_to_remote(host: CHost, local_path: str, remote_path: str,  recursive: bool = False):
    with SSHClient() as ssh:
        ssh.load_system_host_keys()
        ssh.connect(hostname=host.hostname, username=host.username, port=host.port)
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(files=local_path, remote_path=remote_path, recursive=recursive)
            print(f"[INF] copy {SFY(local_path)} to {SFG(remote_path)}")
    return 0
