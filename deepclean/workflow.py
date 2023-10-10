import os

import law
import luigi
from law.contrib import htcondor


class LDGCondorWorkflow(htcondor.HTCondorWorkflow):
    name = luigi.Parameter(default="deepclean")
    condor_directory = luigi.Parameter(default=os.getenv("DATA_DIR"))
    accounting_group_user = luigi.Parameter(default=os.getenv("LIGO_USER"))
    accounting_group = luigi.Parameter(default=os.getenv("LIGO_GROUP"))
    request_disk = luigi.Parameter(default="10 GB")
    request_memory = luigi.Parameter(default="10 GB")

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(self.condor_directory)

    def htcondor_job_config(self, config, job_num, branches):
        config.custom_content.append(("getenv", "true"))
        config.custom_content.append(("request_memory", self.request_memory))
        config.custom_content.append(("request_disk", self.request_disk))
        config.custom_content.append(
            ("accounting_group", self.accounting_group)
        )
        config.custom_content.append(
            ("accounting_group_user", self.accounting_group_user)
        )

        config.custom_content.append(
            (
                "log",
                os.path.join(
                    self.condor_directory, f"{self.name}-$(Cluster).log"
                ),
            )
        )
        config.custom_content.append(
            (
                "output",
                os.path.join(
                    self.condor_directory, f"{self.name}-$(Cluster).out"
                ),
            )
        )
        config.custom_content.append(
            (
                "error",
                os.path.join(
                    self.condor_directory, f"{self.name}-$(Cluster).err"
                ),
            )
        )
        return config
