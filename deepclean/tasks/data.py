import os

import law
import luigi

from deepclean.base import DeepCleanTask


class DataTask(DeepCleanTask):
    @property
    def python(self):
        return "/opt/env/bin/python"

    @property
    def cli(self):
        return [self.python, "/opt/deepclean/projects/data/data"]


class Query(DataTask):
    start = luigi.FloatParameter()
    end = luigi.FloatParameter()
    output_file = luigi.Parameter()
    min_duration = luigi.FloatParameter(default=0)
    flags = luigi.ListParameter(default=["DCS-ANALYSIS_READY_C01:1"])

    def output(self):
        return law.LocalFileTarget(self.output_file)

    @property
    def command(self):
        args = [
            "query",
            "--start",
            str(self.start),
            "--end",
            str(self.end),
            "--output-file",
            self.output().path,
        ]
        for flag in self.flags:
            args.append("--flags+=" + self.ifo + ":" + flag)
        if self.min_duration > 0:
            args.append(f"--min_duration={self.min_duration}")
        return self.cli + args


class Fetch(DataTask, law.LocalWorkflow):
    start = luigi.FloatParameter()
    end = luigi.FloatParameter()
    sample_rate = luigi.FloatParameter()
    data_dir = luigi.Parameter()
    min_duration = luigi.FloatParameter(default=0)
    prefix = luigi.Parameter(default="deepclean")
    flags = luigi.ListParameter(default=["DCS-ANALYSIS_READY_C01:1"])
    segments_file = luigi.Parameter(default=None)

    @law.dynamic_workflow_condition
    def workflow_condition(self) -> bool:
        return self.input()["segments"].exists()

    @workflow_condition.create_branch_map
    def create_branch_map(self):
        segments = self.input()["segments"].load().splitlines()[1:]
        segments = [i.split("\t") for i in segments]
        return dict([(int(i[0]) + 1, i[1::2]) for i in segments])

    @workflow_condition.output
    def output(self):
        start, duration = self.branch_data
        start = int(float(start))
        duration = int(float(duration))
        fname = f"{self.prefix}-{start}-{duration}.hdf5"
        target = law.LocalDirectoryTarget(self.data_dir)
        return target.child(fname, type="f")

    def workflow_requires(self):
        reqs = super().workflow_requires()

        if self.segments_file is None:
            segments_file = os.path.join(self.data_dir, "segments.txt")
        else:
            segments_file = self.segments_file

        reqs["segments"] = Query.req(self, output_file=segments_file)
        return reqs

    @property
    def command(self):
        start, duration = self.branch_data
        start = float(start)
        end = start + float(duration)
        channels = [self.strain_channel] + self.witnesses

        args = [
            "fetch",
            "--start",
            str(start),
            "--end",
            str(end),
            "--sample-rate",
            str(self.sample_rate),
            "--prefix",
            self.prefix,
            "--output-directory",
            self.data_dir,
            "--channels",
            "[" + ",".join(channels) + "]",
        ]
        return self.cli + args
