import { Request, Response } from 'express';
import { DealsService } from '../services/deals.service';

export class DealsController {
  private dealsService: DealsService;

  constructor() {
    this.dealsService = new DealsService();
  }

  async getAllDeals(req: Request, res: Response): Promise<void> {
    try {
      const deals = await this.dealsService.getAllDeals();
      res.json(deals);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch deals' });
    }
  }

  async getDealsByPipeline(req: Request, res: Response): Promise<void> {
    try {
      const { pipelineId } = req.params;
      const deals = await this.dealsService.getDealsByPipeline(pipelineId);
      res.json(deals);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch deals by pipeline' });
    }
  }

  async getDealById(req: Request, res: Response): Promise<void> {
    try {
      const { dealId } = req.params;
      const deal = await this.dealsService.getDealById(dealId);
      res.json(deal);
    } catch (error) {
      res.status(500).json({ error: `Failed to fetch deal ${req.params.dealId}` });
    }
  }

  async createDeal(req: Request, res: Response): Promise<void> {
    try {
      const dealData = req.body;
      const newDeal = await this.dealsService.createDeal(dealData);
      res.status(201).json(newDeal);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create deal' });
    }
  }

  async updateDeal(req: Request, res: Response): Promise<void> {
    try {
      const { dealId } = req.params;
      const dealData = req.body;
      const updatedDeal = await this.dealsService.updateDeal(dealId, dealData);
      res.json(updatedDeal);
    } catch (error) {
      res.status(500).json({ error: `Failed to update deal ${req.params.dealId}` });
    }
  }
}