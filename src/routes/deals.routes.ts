import { Router } from 'express';
import { DealsController } from '../controllers/deals.controller';

const router = Router();
const dealsController = new DealsController();

router.get('/', (req, res) => dealsController.getAllDeals(req, res));
router.get('/pipeline/:pipelineId', (req, res) => dealsController.getDealsByPipeline(req, res));
router.get('/:dealId', (req, res) => dealsController.getDealById(req, res));
router.post('/', (req, res) => dealsController.createDeal(req, res));
router.put('/:dealId', (req, res) => dealsController.updateDeal(req, res));

export default router;