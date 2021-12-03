import json
import os
import shutil

import yaml


def validate_outputs(files, name, samples=[]):
    if 'metadata.yaml' in files:
        files.remove('metadata.yaml')

    if name == 'hmmcopy':
        expected_files = [
            'hmmcopy_params.csv.gz', 'hmmcopy_params.csv.gz.yaml',
            'hmmcopy_segments.csv.gz', 'hmmcopy_segments.csv.gz.yaml',
            'hmmcopy_reads.csv.gz', 'hmmcopy_reads.csv.gz.yaml',
            'hmmcopy_metrics.csv.gz', 'hmmcopy_metrics.csv.gz.yaml',
            'hmmcopy_segments_pass.tar.gz', 'hmmcopy_segments_fail.tar.gz',
            'hmmcopy_heatmap.pdf', 'input.json', 'qc_html_report.html'
        ]
        assert sorted(files) == sorted(expected_files)
    elif name == 'alignment':
        expected_files = [
            'alignment_gc_metrics.csv.gz', 'alignment_gc_metrics.csv.gz.yaml', 'alignment_metrics.csv.gz',
            'alignment_metrics.csv.gz.yaml', 'alignment_metrics.tar.gz', 'all_cells_bulk.bam', 'all_cells_bulk.bam.bai',
            'all_cells_bulk_contaminated.bam', 'all_cells_bulk_contaminated.bam.bai', 'all_cells_bulk_control.bam',
            'all_cells_bulk_control.bam.bai', 'detailed_fastqscreen_breakdown.csv.gz',
            'detailed_fastqscreen_breakdown.csv.gz.yaml', 'input.json'
        ]
        assert sorted(files) == sorted(expected_files)
    elif name == 'breakpoint_calling':
        expected_files = ['four_way_consensus.csv.gz', 'four_way_consensus.csv.gz.yaml',
                          'input.json']
        for sample in samples:
            expected_files += [
                '{}_breakpoint_library_table.csv'.format(sample),
                '{}_breakpoint_read_table.csv'.format(sample),
                '{}_breakpoint_table.csv'.format(sample),
                '{}_gridss.vcf.gz'.format(sample),
                '{}_lumpy.vcf'.format(sample),
                '{}.svaba.somatic.sv.vcf.gz'.format(sample)
            ]
        assert sorted(files) == sorted(expected_files)
    elif name == 'variant_calling':
        expected_files = [
            'final_maf_all_samples.maf',
            'final_vcf_all_samples.vcf.gz',
            'final_vcf_all_samples.vcf.gz.csi',
            'final_vcf_all_samples.vcf.gz.tbi',
            'input.json'
        ]
        for sample in samples:
            expected_files += [
                '{}_consensus.vcf.gz'.format(sample),
                '{}_consensus.vcf.gz.csi'.format(sample),
                '{}_consensus.vcf.gz.tbi'.format(sample),
                '{}_museq.vcf.gz'.format(sample),
                '{}_museq.vcf.gz.csi'.format(sample),
                '{}_museq.vcf.gz.tbi'.format(sample),
                '{}_mutect.vcf.gz'.format(sample),
                '{}_mutect.vcf.gz.csi'.format(sample),
                '{}_mutect.vcf.gz.tbi'.format(sample),
                '{}_strelka_indel.vcf.gz'.format(sample),
                '{}_strelka_indel.vcf.gz.csi'.format(sample),
                '{}_strelka_indel.vcf.gz.tbi'.format(sample),
                '{}_strelka_snv.vcf.gz'.format(sample),
                '{}_strelka_snv.vcf.gz.csi'.format(sample),
                '{}_strelka_snv.vcf.gz.tbi'.format(sample),
                '{}_updated_counts.maf'.format(sample)
            ]
        assert sorted(files) == sorted(expected_files)
    elif name == 'snv_genotyping':
        expected_files = ['snv_genotyping.csv.gz', 'snv_genotyping.csv.gz.yaml', 'input.json']
        assert sorted(files) == sorted(expected_files)
    else:
        raise NotImplementedError()


def extract_name_version(wdl_file):
    with open(wdl_file, 'rt') as wdl_reader:
        pipeline_version = wdl_reader.readline()

        if pipeline_version.startswith('#{"meta"'):
            pipeline_version = pipeline_version[1:]
            pipeline_version = json.loads(pipeline_version)['meta']
            return pipeline_version['name'], pipeline_version['version']
        else:
            return None, None


def load_options_json(options_json):
    data = json.load(open(options_json, 'rt'))

    return {
        'wf_logs': data['final_workflow_log_dir'],
        'out_dir': data['final_workflow_outputs_dir'],
    }


def get_all_outputs(outdir):
    files = os.listdir(outdir)
    for file in files:
        if os.path.isdir(file):
            raise Exception("dir found in outputdir")

    return files


def _get_samples_pseudobulk(input_json):
    data = json.load(open(input_json, 'rt'))

    for key in data:
        if 'samples' in key:
            return list(data[key].keys())

    raise Exception('unable to find samples in input json')


def _get_alignment_samples(input_json):
    data = json.load(open(input_json, 'rt'))

    samples = set()
    for sample in data['AlignmentWorkflow.fastq_files']:
        samples.append(sample['sample_id'])

    return sorted(samples)


def _get_alignment_libraries(input_json):
    data = json.load(open(input_json, 'rt'))

    libraries = set()
    for sample in data['AlignmentWorkflow.fastq_files']:
        libraries.add(sample['library_id'])

    return sorted(libraries)


def _get_alignment_lanes(input_json):
    data = json.load(open(input_json, 'rt'))

    lanes = set()
    for sample in data['AlignmentWorkflow.fastq_files']:
        for lane in sample['lanes']:
            lanes.add(lane['lane_id'])

    return sorted(lanes)


def get_sample_ids(input_json, name):
    if name in ['breakpoint_calling', 'variant_calling', 'snv_genotyping']:
        return _get_samples_pseudobulk(input_json)
    elif name == 'alignment':
        return _get_alignment_samples(input_json)
    elif name == 'hmmcopy':
        return []
    else:
        raise Exception('Unknown pipeline: {}'.format(name))


def create_metadata_yaml(outdir, pipeline_wdl, input_json, yamlfile):
    name, version = extract_name_version(pipeline_wdl)
    files = get_all_outputs(outdir)
    samples = get_sample_ids(input_json, name)

    validate_outputs(files, name, samples=samples)

    data = {
        'filenames': files,
        'meta': {
            'version': version,
            'name': name,
            'sample_ids': samples
        }
    }

    if name == 'alignment':
        data['meta']['library_ids'] = _get_alignment_libraries(input_json)
        data['meta']['lane_ids'] = _get_alignment_lanes(input_json)

    with open(yamlfile, 'wt') as writer:
        yaml.dump(data, writer, default_flow_style=False)


def add_metadata(options_json, input_json, pipeline_wdl):
    options_data = load_options_json(options_json)

    out_dir = options_data['out_dir']

    shutil.copyfile(input_json, os.path.join(out_dir, "input.json"))

    create_metadata_yaml(out_dir, pipeline_wdl, input_json, os.path.join(out_dir, "metadata.yaml"))
