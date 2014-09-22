

from mrjob.job import MRJob

from mrjob.protocol import JSONValueProtocol



try:
    import simplejson as json
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json




import re



class MRExtractFromJson(MRJob):


    OUTPUT_PROTOCOL = JSONValueProtocol


    def configure_options(self):
            super(MRExtractFromJson, self).configure_options()

            self.add_passthrough_option(
                '--features', default='subject')



    def mapper_init(self):
        self.features_list = self.options.features.split(',')


    def mapper(self, _, line):
        data = json.loads(line)
        key = data['_id']['$oid']

        extracted_data = {}
        extracted_data['key'] = key

        for item in self.features_list:

            if item == 'subject':
                if 'subject' in data and data['subject'] is not None:
                    if isinstance(data['subject'], list):
                        before = ' '.join(str(item) for item in data['subject'])
                        before = before.strip().encode('ascii', 'ignore')
                    else:
                        before = data['subject'].strip().encode('ascii', 'ignore')
                    re.sub('\s+',' ', before)
                    re.sub('\t',' ', before)
                    before = before.strip()
                    if len(before) > 1:
                        extracted_data[item] = ' '.join(before.split())

            if item == 'geo':
                if 'long' in data and 'lat' in data and data['long'] is not None and data['lat'] is not None \
                        and len(str(data['long']))>0 and len(str(data['lat']))>0:
                    extracted_data[item] = (str(data['long']) +'\t'+ str(data['lat']))

            if item == 'source_ip':
                if 'source_ip' in data and data['source_ip'] is not None and len(str(data['source_ip'])) > 0:
                    extracted_data[item] = str(data['source_ip'])

            if item == 'IP':
                if 'IP' in data and data['IP'] is not None and len(str(data['IP'])) > 0:
                    extracted_data[item] = str(data['IP'])

            if item == 'av_avira':
                if 'av_avira' in data and data['av_avira'] is not None:
                    extracted_data[item] = str(data['av_avira'])

            if item == 'file_md5':
                if 'file_md5' in data and data['file_md5'] is not None:
                    extracted_data[item] = str(data['file_md5'])

            if item == 'date':
                if 'date' in data and data['date'] is not None:
                    extracted_data[item] = str(data['date'])

            if item == 'file_ssdeep':
                if 'file_ssdeep' in data and data['file_ssdeep'] is not None:
                    extracted_data[item] = str(data['file_ssdeep'])

            if item == 'uri_domain':
                if 'uri_domain' in data and data['uri_domain'] is not None and len(data['uri_domain']) > 0:
                    extracted_data[item] = data['uri_domain']

            if item == 'file_name':
                if 'file_name' in data and data['file_name'] is not None and len(data['file_name']) > 0:
                    extracted_data[item] = data['file_name']

            if item == 'from_address':
                if 'from_address' in data and data['from_address'] is not None and len(data['from_address']) > 0:
                    extracted_data[item] = data['from_address']


            if item == 'to_domain':
                if 'to_domain' in data and data['to_domain'] is not None and len(data['to_domain']) > 0:
                    extracted_data[item] = data['to_domain']


            if item == 'nw_connect':
                if 'nw_connect' in data and data['nw_connect'] is not None and len(data['nw_connect']) > 0:
                    extracted_data[item] = data['nw_connect']

            if item == 'bot':
                if 'bot' in data and data['bot'] is not None and len(data['bot']) > 0:
                    extracted_data[item] = data['bot']


            if item == 'day':
                if 'day' in data and data['day'] is not None and len(data['day']) > 0:
                    extracted_data[item] = data['day']


            if item == 'charset':
                if 'charset' in data and data['charset'] is not None and len(data['charset']) > 0:
                    extracted_data[item] = data['charset']

            if item == 'country_code':
                if 'country_code' in data and data['country_code'] is not None and len(data['country_code']) > 0:
                    extracted_data[item] = data['country_code']


            if item == 'host':
                if 'host' in data and data['host'] is not None and len(data['host']) > 0:
                    extracted_data[item] = data['host']

            if item == 'cte':
                if 'cte' in data and data['cte'] is not None and len(data['cte']) > 0:
                    extracted_data[item] = data['cte']

            if item == 'x_p0f_detail':
                if 'x_p0f_detail' in data and data['x_p0f_detail'] is not None and len(data['x_p0f_detail']) > 0:
                    extracted_data[item] = data['x_p0f_detail']



        yield None, extracted_data




if __name__ == '__main__':

    MRExtractFromJson.run()
