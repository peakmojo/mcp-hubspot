import { Client } from '@hubspot/api-client';
import { SimplePublicObjectWithAssociations } from '@hubspot/api-client/lib/codegen/crm/deals';
import { hubspotClient } from '../config/hubspot.config';

export class DealsService {
  private client: Client;

  constructor() {
    this.client = hubspotClient;
  }

  async getAllDeals(): Promise<SimplePublicObjectWithAssociations[]> {
    try {
      const apiResponse = await this.client.crm.deals.getAll();
      return apiResponse.results;
    } catch (error) {
      console.error('Error fetching deals:', error);
      throw error;
    }
  }

  async getDealsByPipeline(pipelineId: string): Promise<SimplePublicObjectWithAssociations[]> {
    try {
      const filter = {
        filterGroups: [{
          filters: [{
            propertyName: 'pipeline',
            operator: 'EQ',
            value: pipelineId
          }]
        }]
      };
      
      const apiResponse = await this.client.crm.deals.searchApi.doSearch(filter);
      return apiResponse.results;
    } catch (error) {
      console.error('Error fetching deals by pipeline:', error);
      throw error;
    }
  }

  async getDealById(dealId: string): Promise<SimplePublicObjectWithAssociations> {
    try {
      return await this.client.crm.deals.basicApi.getById(dealId);
    } catch (error) {
      console.error(`Error fetching deal ${dealId}:`, error);
      throw error;
    }
  }

  async createDeal(dealData: any): Promise<SimplePublicObjectWithAssociations> {
    try {
      return await this.client.crm.deals.basicApi.create({ properties: dealData });
    } catch (error) {
      console.error('Error creating deal:', error);
      throw error;
    }
  }

  async updateDeal(dealId: string, dealData: any): Promise<SimplePublicObjectWithAssociations> {
    try {
      return await this.client.crm.deals.basicApi.update(dealId, { properties: dealData });
    } catch (error) {
      console.error(`Error updating deal ${dealId}:`, error);
      throw error;
    }
  }
}