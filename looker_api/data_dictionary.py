class LookerDataDictionary:
    def __init__(self, api, fieldnames=['id, name, description, fields'], relevant_models=None):
        # Looker User API client
        self.api = api
        self.fieldnames = fieldnames
        self.relevant_models = relevant_models
        self.data_dictionary = {}
        self.models = self.get_models()

    def get_models(self):
        models = self.api.all_lookml_models()
        if self.relevant_models:
            models = [model for model in models if model.name in self.relevant_models]
            return models
        return models

    def construct_data_dict(self):
        for model in self.models:
            self.construct_data_dict_for_model(model)

    def construct_data_dict_for_model(self, model):
        explore_names = [explore.name for explore in model.explores]
        for explore in explore_names:
            fields = self.api.lookml_model_explore(explore_name=explore, lookml_model_name=model.name)
            self.add_fields_to_data_dictionary(fields, explore)

    def add_field_dict(self, field, explore, field_type):
        if field.name in self.data_dictionary.keys():
            self.data_dictionary[field.name]['explores'].add(explore)
        else:
            field_dict = {
                'data_type': field.type,
                'description': field.description,
                'explores': {explore},
                'field_type': field_type,
                'hidden': field.hidden,
                'name': field.name,
                'label': field.label_short,
                'sql_query': field.sql,
                'view_name': field.view_label,
                'view': field.view,
            }
            self.data_dictionary[field.name] = field_dict

    def add_fields_to_data_dictionary(self, fields, explore):
        for dimension in fields.fields.dimensions:
            self.add_field_dict(dimension, explore, 'dimension')

        for measure in fields.fields.measures:
            self.add_field_dict(measure, explore, 'measure')