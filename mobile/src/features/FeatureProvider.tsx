import {createContext} from 'react';
import type {FeatureContext} from './types';
import type {Message} from '../types';

export const FeatureProvider = createContext<{context:FeatureContext;events:Message[]}|null>(null);
