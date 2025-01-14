import express from 'express';
import contactsRouter from './routes/contacts.routes';
import companiesRouter from './routes/companies.routes';
import dealsRouter from './routes/deals.routes';

const app = express();

app.use(express.json());

// Routes
app.use('/contacts', contactsRouter);
app.use('/companies', companiesRouter);
app.use('/deals', dealsRouter);

export default app;