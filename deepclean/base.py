import os
import shlex
import subprocess
from collections.abc import Callable
from pathlib import Path

import law
import luigi
from law.contrib import singularity

from deepclean.config import deepclean as Config

root = Path(__file__).resolve().parent.parent


class DeepCleanSandbox(singularity.SingularitySandbox):
    sandbox_type = "deepclean"

    def _get_volumes(self):
        volumes = super()._get_volumes()
        fnames = [
            "/etc/krb5.conf",
            "/etc/krb5.conf.d",
            "/etc/pki/tls/certs/ca-bundle.crt",
        ]
        for fname in fnames:
            volumes[fname] = fname

        for dir in ["/etc/condor", "/cvmfs"]:
            volumes[dir] = dir

        if self.task and getattr(self.task, "dev", False):
            volumes[str(root)] = "/opt/deepclean"
        return volumes


class DeepCleanTask(law.SandboxTask):
    image = luigi.Parameter()
    dev = luigi.BoolParameter(default=False)
    gpus = luigi.Parameter(default="")

    cfg = Config()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not os.path.isabs(self.image):
            self.image = os.path.join(Config().container_root, self.image)

        if not os.path.exists(self.image):
            raise ValueError(
                f"Could not find path to container image {self.image}"
            )

    @property
    def ifo(self):
        return self.cfg.ifo.value

    @property
    def strain_channel(self):
        return f"{self.ifo}:{self.cfg.strain_channel}"

    @property
    def witnesses(self):
        return self.cfg.channels

    @property
    def sandbox(self):
        return f"deepclean::{self.image}"

    @property
    def singularity_args(self) -> Callable:
        def arg_getter():
            if self.gpus:
                return ["--nv"]
            return []

        return arg_getter

    def sandbox_env(self, _):
        if self.gpus:
            return {"CUDA_VISIBLE_DEVICES": self.gpus}
        return {}

    @property
    def python(self) -> str:
        return "/usr/local/bin/python"

    @property
    def command(self) -> str:
        return [self.python, "-c", "print('Hello world')"]

    def run(self):
        try:
            subprocess.run(
                self.command, capture_output=True, check=True, text=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "Command '{}' failed with return code {} "
                "and stderr:\n{}".format(
                    shlex.join(e.cmd), e.returncode, e.stderr
                )
            ) from None
