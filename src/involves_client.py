from requests import Session
from requests.auth import HTTPBasicAuth
from src.utilities import extract_json_keys


class InvolvesAPIClient(Session):

    def __init__(self,
                 environment,
                 domain,
                 username,
                 password):

        super().__init__()
        
        self.environment = environment
        self.username = username
        self.password = password
        self.auth = HTTPBasicAuth(self.username,self.password)
        self.base_url = f'https://{domain}.involves.com/webservices/api' 

        self.headers.update({

        'X-AGILE-CLIENT': 'EXTERNAL_APP',
        'Accept-Version': '2020-02-26'
        }
        )

    def request(self, url, method, params=None):

        response = super().request(url=url,method=method,headers=self.headers,params=params)

        response.raise_for_status()

        if method == 'GET':

            if params:

                params.update({
                    'size':200
                })
            
            else:

                params = {
                    'size':200
                }


            if 'totalPages' in response.json():

                    rows = []
                    page = 1
                    totalPages = response.json().get('totalPages')

                    while page <= totalPages:
                         
                         params.update({
                              'page' : page
                            })
                         
                         response = super().request(url=url,method=method, headers=self.headers, params=params)

                         if 'items' in response.json():
                              
                            rows.extend(response.json().get('items'))
                        
                         elif 'itens' in response.json():

                            rows.extend(response.json().get('itens'))
                        
                         page += 1

                    return rows
            else:

                return response.json()
    
    @staticmethod
    def create_params(**kwargs):

        params = {}

        for key, value in kwargs.items():

            if value is not None and value != 'select':

                params[key] = value
        
        return params
    
    @staticmethod
    def select_fields(data,fields):

        if fields:
        
            if isinstance(data,list):
                return [
                    {
                        key: extract_json_keys(value,key) for key in fields
                        }
                    for value in data
                ]
            if isinstance(data,dict):

                result = {}

                for key in fields:
                    nested_keys = key.split('_') 
                    temp = data

                    for nested_key in nested_keys:
                        if nested_key in temp:
                            temp = temp[nested_key]

                        else:
                            temp = None
                            break

                    result[key] = temp

                return result
            else:
                return data

    def get_pointsofsale(self,name=None,active=None,updatedAtMillis=None,select=None):


        params = self.create_params(name=name,active=active,updatedAtMillis=updatedAtMillis)
        
        data = self.request(url=f'{self.base_url}/v1/{self.environment}/pointofsale',method='GET',params=params)

        return self.select_fields(data=data,fields=select) 
    
    def get_pointofsale_by_id(self,pointOfSaleId):

        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/pointofsales/{pointOfSaleId}',method='GET')
            
    
    def get_products(self,name=None,active=None):

        params = self.create_params(name=name,active=active)
        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/skus',method='GET',params=params)
    
    def get_product_by_id(self,skuId):

        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/skus/{skuId}',method='GET')
    
    def get_product_lines(self):

        return self.request(url=f'{self.base_url}/v1/{self.environment}/productline/find')

    def get_product_supercategories(self):
        
       return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/supercategories')

    def get_product_brands(self):

        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/supercategories')

    def get_product_category_by_id(self,categoryId):

        return self.requests(url=f'{self.base_url}/v3/environments/{self.environment}/categories/{categoryId}')
    
    
    def get_employees(self,regionId=None,formId=None,name=None,active=None,updatedAtMillis=None,select=None):

        params = self.create_params(regionId=regionId,formId=formId,name=name,active=active,updatedAtMillis=updatedAtMillis)
        data = self.request(url=f'{self.base_url}/v1/{self.environment}/employeeenvironment',method='GET',params=params)

        return self.select_fields(data=data,fields=select) 
    
    def get_employee_by_id(self,employeeId):

        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/employees/{employeeId}',method='GET')
    
    def get_regions(self,name=None,employeeId=None):

        params = self.create_params(name=name,employeeId=employeeId)
        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/regionals',method='GET',params=params)
    
    def get_region_by_id(self,regionalId):

        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/regionals/{regionalId}',method='GET')
    
    def get_employee_absences(self,employeeEnvironmentId=None,startDate=None,endDate=None):

        params = self.create_params(employeeEnvironmentId,startDate,endDate)
        return self.request(url=f'{self.base_url}/v1/{self.environment}/employeeabsence',method='GET',params=params)


    def get_activated_forms(self):

        return self.request(url=f'{self.base_url}/v1/{self.environment}/form/formsActivated',method='GET')


    def get_fields_by_form_id(self,formId):

        return self.request(url=f'{self.base_url}/v1/{self.environment}/form/formFields/{formId}',method='GET')


    def get_form_responses(self,ownerId,status,formIds):

        params = self.create_params(ownerId,status,formIds)
        return self.request(url=f'{self.base_url}/v3/environments/{self.environment}/surveys',method='GET',params=params)

    def get_form_response_by_id(self,surveyId):

        return self.request(url=f'/v3/environments/{self.environment}/surveys/{surveyId}',method='GET')



