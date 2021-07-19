#!/usr/bin/env python
import logging
import sys
import datetime
import hashlib

class Participant_Assessments():
    
     def __init__(self, schemaMappings):

        logging.basicConfig(level=logging.INFO)
        self.schemaMappings = schemaMappings
        
     def build_participantAssessments_dataset(self, valid_assessment_datasets, valid_participant_data, data_visibility,
                                              community_id, tool_id, version, contacts) :
        logging.info(
           "\n\t==================================\n\t5. Processing Participant Assessments dataset\n\t==================================\n")
        
        # name and description
        dataset_name = "Participant Assessments results"
        
        dataset_description = "Participant Assessments dataset for "\
                + valid_participant_data["_id"] + "' participant"
      
        #challenges and contacts
        execution_challenges = set()
        contacts_ids = set()
        cadena = valid_participant_data['_id']+":"
        rel_dataset_ids = []
        rel_dataset_ids.append({
                    'dataset_id': valid_participant_data['_id'],
                    'role': 'dependency'
        })
        for dataset in valid_assessment_datasets:
            for challenge_id in dataset['challenge_ids']:
                execution_challenges.add(challenge_id)
                
            for dataset_contact_id in dataset['dataset_contact_ids']:
                contacts_ids.add(dataset_contact_id)
                
            rel_dataset_ids.append({
                    'dataset_id': dataset['_id'],
                    'role': 'dependency'
            })
            cadena += dataset.get('_id')+":"
        
        #Generate unique origin id
        cadena += tool_id
        dataset_id = hashlib.md5(cadena.encode('utf-8'))
        
      
        participantAssessments_dataset = {

            #QfO:NEW_PREDICTOR_P:QfO:GO_NR_ORTHOLOGS_NEW_PREDICTOR_A:QfO:GO_avg Schlicker_NEW_PREDICTOR_A_OEBT0020000007_OEBX002000000B
            '_id': dataset_id.hexdigest()+"_ParticipantAssessments",
            '_schema': self.schemaMappings["Dataset"],
            'type': 'participant_assessments',
            'challenge_ids': list(execution_challenges),
            'visibility': data_visibility,
            'name': dataset_name,
            'description': dataset_description,
            'version': str(version),
            'dates': {
                'creation': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
                'modification': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
            },
            'datalink': {
                'uri': 'oeb:{}'.format(tool_id),
                'attrs': [
                    'curie'
                ]
            },
            'depends_on': {
                'rel_dataset_ids': rel_dataset_ids,
            },
            "dataset_contact_ids": list(contacts_ids)
            
        }
        
        sys.stdout.write(
            'Processed Participant Assessments dataset...\n')
        
        return participantAssessments_dataset
    
     def build_participantAssessments_events(self, assessment_datasets, valid_participantAssessments_data):
         logging.info(
             "\n\t==================================\n\t6. Generating participantAssessments Events\n\t==================================\n")
        
         # initialize the array of test events
         participantAssessments_events = []
         for dataset in assessment_datasets:
             
             involved_data = []
             involved_data.append({
                    "dataset_id": dataset['_id'],
                    "role": "incoming"
             })
             involved_data.append({
                    "dataset_id": valid_participantAssessments_data['_id'],
                    "role": "outgoing"
             })
             for challenge in dataset["challenge_ids"]:
                 orig_id = dataset.get("orig_id",dataset["_id"])
                 event_id = orig_id + "_ParticipantAssessmentsEvent"
                 event = {
                    '_id': event_id,
                    '_schema': self.schemaMappings["TestAction"],
                    'action_type': 'ParticipantAssessmentsEvent',
                    'dates': {
                        'creation': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
                        'modification': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
                    },
                    'test_contact_ids': dataset['dataset_contact_ids'],
                    'involved_datasets': involved_data,
                    'challenge_id': challenge
                 }
                 participantAssessments_events.append(event)


         return participantAssessments_events
     
     
