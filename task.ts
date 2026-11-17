import { Feature } from '@tak-ps/node-cot'
import { Static, Type, TSchema } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const InputSchema = Type.Object({
    'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
    'Point Geometries': Type.Boolean({ description: 'Allow point geometries', default: true }),
    'LineString Geometries': Type.Boolean({ description: 'Allow LineString geometries', default: true }),
    'Polygon Geometries': Type.Boolean({ description: 'Allow Polygon Geometries', default: true }),
    'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
});

const SignGeometry = Type.Union([
    Feature.Point,
    Feature.MultiPoint,
    Feature.LineString,
    Feature.MultiLineString,
    Feature.Polygon,
    Feature.MultiPolygon
]);

const SignResponse = Type.Object({
    features: Type.Array(Type.Object({
        properties: Type.Object({
            id: Type.Union([Type.String(), Type.Number()])
        }),
        geometry: Type.Unknown()
    }))
});

function splitGeometry(geometry: Static<typeof SignGeometry>): Array<Static<typeof Feature.Geometry>> {
    if (geometry.type === 'MultiPoint') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Point', coordinates }));
    }

    if (geometry.type === 'MultiLineString') {
        return geometry.coordinates.map((coordinates) => ({ type: 'LineString', coordinates }));
    }

    if (geometry.type === 'MultiPolygon') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Polygon', coordinates }));
    }

    return [ geometry ];
}

export default class Task extends ETL {
    static name = 'etl-cotrip-signs';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow !== DataFlowType.Incoming) return Type.Object({});

        if (type === SchemaType.Input) return InputSchema;

        return Type.Object({
            communicationStatus: Type.String(),
            marker: Type.Number(),
            messageText: Type.String(),
            direction: Type.String(),
            lastUpdated: Type.String(),
            messagePreview: Type.String(),
            displayStatus: Type.String(),
            name: Type.String(),
            id: Type.String(),
            speed: Type.Number(),
            routeName: Type.String(),
            messageMarkup: Type.String(),
            publicName: Type.String(),
            submittedBy: Type.String(),
            nativeId: Type.String(),
            activationTime: Type.String(),
        });
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        const api = 'https://data.cotrip.org/';
        if (!env.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = env.COTRIP_TOKEN;

        const signs: Static<typeof SignResponse>['features'] = [];
        let batch = -1;
        let res: Response | undefined;
        do {
            console.log(`ok - fetching ${++batch} of signs`);
            const url = new URL('/api/v1/signs', api);
            url.searchParams.append('apiKey', token);
            if (res) {
                const nextOffset = res.headers.get('next-offset');
                if (nextOffset) url.searchParams.append('offset', nextOffset);
            }

            res = await fetch(url);

            const body: unknown = await res.json();
            if (!Value.Check(SignResponse, body)) throw new Error('Unexpected COTrip Signs response');

            signs.push(...body.features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${signs.length} signs`);

        const features: Array<Static<typeof Feature.InputFeature>> = [];
        for (const sign of signs) {
            console.error(sign)

            if (!Value.Check(SignGeometry, sign.geometry)) continue;

            const id = String(sign.properties.id);
            const multi = sign.geometry.type.startsWith('Multi');

            splitGeometry(sign.geometry).forEach((geometry, i) => {
                features.push({
                    id: multi ? `${id}-${i}` : id,
                    type: 'Feature',
                    properties: geometry.type === 'Point' ? {} : {
                        type: 'u-d-f'
                    },
                    geometry
                });
            });
        }

        const allowed: string[] = [];
        if (env['Point Geometries']) allowed.push('Point');
        if (env['LineString Geometries']) allowed.push('LineString');
        if (env['Polygon Geometries']) allowed.push('Polygon');

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features.filter((feat) => {
                return allowed.includes(feat.geometry.type);
            })
        };

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}
